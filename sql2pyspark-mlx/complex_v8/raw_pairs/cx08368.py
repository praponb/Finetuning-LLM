from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_moved_at_to_date = Window.partitionBy(F.to_date("moved_at"))
w_sku_id_by_first_moved_at_tie = Window.partitionBy("sku_id").orderBy("first_moved_at")
w_sku_id_by_first_moved_at = Window.partitionBy("sku_id").orderBy("first_moved_at", "id")
w_moved_at_month_by_moved_at = Window.partitionBy(F.month("moved_at")).orderBy(F.col("moved_at").desc(), "id")

filtered = (
    df_stock_moves
    .filter((F.col("unit_price") < 367.7) & (F.col("sku_id") <= 16))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "unit_price",
        F.substring(F.trim(F.col("move_type")), 1, 2).alias("move_type_prefix"),
        F.concat(F.col("move_type"), F.lit("#"), F.col("warehouse_id").cast("string")).alias("move_type_ref"),
        F.round(F.col("qty") / F.nullif(F.col("unit_price"), F.lit(0)), 3).alias("qty_per_unit_price"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(
        F.min("moved_at").alias("first_moved_at"),
        F.max("unit_price").alias("max_move_unit_price"),
    )
)

profiled = (
    filtered.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select("s.*", "st.first_moved_at", "st.max_move_unit_price")
)

metrics = (
    profiled
    .select(
        "*",
        F.max("unit_price").over(w_moved_at_to_date).alias("moved_at_to_date_max_unit_price"),
        F.rank().over(w_sku_id_by_first_moved_at_tie).alias("first_moved_at_rank"),
        F.ntile(5).over(w_sku_id_by_first_moved_at).alias("first_moved_at_quintile"),
        F.ntile(4).over(w_moved_at_month_by_moved_at).alias("moved_at_quartile"),
        F.lag("qty", 2).over(w_sku_id_by_first_moved_at).alias("prev2_qty"),
        F.round(F.cume_dist().over(w_sku_id_by_first_moved_at), 3).alias("first_moved_at_cume_dist"),
    )
)

detail_rows = (
    metrics.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.moved_at",
        "s.unit_price",
        "s.move_type_prefix",
        "s.move_type_ref",
        "s.first_moved_at",
        "s.max_move_unit_price",
        "s.first_moved_at_rank",
        "s.first_moved_at_quintile",
        "s.prev2_qty",
        F.col("st.id").alias("move_id"),
        F.col("st.operator").alias("move_operator"),
        F.col("st.sku_id").alias("move_sku_id"),
        F.col("st.unit_price").alias("move_unit_price"),
        F.col("st.qty").alias("move_qty"),
    )
)

with_details_move = (
    detail_rows.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.id") == F.col("s.move_id"), "left")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .select(
        "s.*",
        F.col("st.sku_id").alias("move_sku_id_2"),
        F.col("st.moved_at").alias("move_moved_at"),
        F.col("st.move_type").alias("move_move_type"),
        F.col("sk.labels").alias("sku_labels"),
        F.col("sk.unit_cost").alias("sku_unit_cost"),
    )
)

combined_details_sku = (
    with_details_move.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.move_sku_id"), "inner")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .filter(F.col("sk.unit_cost") > 38.3)
    .select(
        "s.*",
        F.col("sk.unit_cost").alias("sku_unit_cost_2"),
        "sk.sku_code",
        F.col("sk.weight_kg").alias("sku_weight_kg"),
        F.col("w.wh_code").alias("warehouse_wh_code"),
    )
)

result = (
    combined_details_sku
    .select(
        "id",
        "sku_id",
        "moved_at",
        "unit_price",
        "move_type_prefix",
        "move_type_ref",
        "first_moved_at",
        "max_move_unit_price",
        "first_moved_at_rank",
        "prev2_qty",
        "move_sku_id",
        "move_unit_price",
        "move_qty",
        "move_sku_id_2",
        "move_moved_at",
        "move_move_type",
        "sku_labels",
        "sku_unit_cost",
        "sku_unit_cost_2",
        "sku_weight_kg",
        "warehouse_wh_code",
    )
    .orderBy("move_sku_id", F.col("moved_at").asc())
)
