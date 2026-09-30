from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_label = Window.partitionBy("label")
w_move_moved_at_month_by_move = Window.partitionBy("move_moved_at_month").orderBy(F.col("move_qty").desc(), "id", "move_id", "label").rowsBetween(-1, -1)

scoped = (
    df_skus
    .filter((F.array_contains(F.col("labels"), "express")) & (~F.col("category").isin("Beauty", "Toys")))
    .select(
        "id",
        "sku_code",
        "weight_kg",
        "labels",
        "is_active",
        F.concat(F.col("category"), F.lit("#"), F.col("id").cast("string")).alias("category_ref"),
    )
)

without_matches = (
    scoped.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.id")) & (F.col("st.warehouse_id") >= 3) & (F.col("st.unit_price").between(58.6, 328.3)), "left_semi")
)

with_children_moves = (
    without_matches.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.id")) & (F.col("st.moved_at") >= F.lit("2025-01-31 08:00:00").cast("timestamp")), "left")
    .select(
        "s.id",
        "s.weight_kg",
        "s.labels",
        "s.category_ref",
        F.col("st.id").alias("move_id"),
        F.col("st.moved_at").alias("move_moved_at"),
        F.col("st.operator").alias("move_operator"),
        F.col("st.warehouse_id").alias("move_warehouse_id"),
        F.col("st.qty").alias("move_qty"),
    )
)

per_label = (
    with_children_moves
    .select(
        "id",
        "weight_kg",
        "category_ref",
        "move_id",
        "move_moved_at",
        "move_operator",
        "move_warehouse_id",
        "move_qty",
        F.explode("labels").alias("label"),
    )
)

tagged = (
    per_label
    .select(
        "id",
        "weight_kg",
        "move_id",
        "move_moved_at",
        "move_operator",
        "move_warehouse_id",
        "move_qty",
        "label",
        F.month(F.col("move_moved_at")).alias("move_moved_at_month"),
        F.coalesce(F.col("label"), F.lit("UNKNOWN")).alias("label_or_unknown"),
    )
)

windowed = (
    tagged
    .select(
        "*",
        F.round(F.col("move_moved_at_month") / F.nullif(F.sum("move_moved_at_month").over(w_label), F.lit(0)), 4).alias("move_moved_at_month_share_of_label"),
        F.min("move_moved_at_month").over(w_label).alias("label_min_move_moved_at_month"),
        F.count(F.lit(1)).over(w_label).alias("rows_in_label"),
        F.max("move_moved_at_month").over(w_move_moved_at_month_by_move).alias("move_moved_at_month_max_1_prior"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(F.count("*").alias("move_count"))
)

with_stats = (
    windowed.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.move_warehouse_id"), "left")
    .select("s.*", F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"))
)

result = (
    with_stats
    .filter(F.col("move_moved_at_month_share_of_label") > 0.3)
    .select(
        "id",
        "weight_kg",
        "move_id",
        "move_moved_at",
        "move_operator",
        "move_warehouse_id",
        "move_qty",
        "move_moved_at_month",
        "label_or_unknown",
        "move_moved_at_month_share_of_label",
        "label_min_move_moved_at_month",
        "rows_in_label",
        "move_moved_at_month_max_1_prior",
        "move_count",
        F.when(F.col("label").isin("bulky", "cold"), "group_a").when(F.col("label") == "express", "group_b").otherwise("other").alias("label_group"),
        F.round(F.col("weight_kg") * 0.25, 2).alias("weight_kg_adj"),
    )
    .orderBy(F.col("move_warehouse_id").asc())
)
