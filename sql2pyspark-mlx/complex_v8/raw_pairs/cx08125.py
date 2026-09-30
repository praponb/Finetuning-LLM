from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_id_warehouse_id_by_moved = Window.partitionBy("sku_id", "warehouse_id").orderBy(F.col("moved_at_hour").desc(), "id").rowsBetween(-3, -1)
w_operator_warehouse_id = Window.partitionBy("operator", "warehouse_id")
w_all_by_moved_at = Window.orderBy(F.col("moved_at").desc(), "id")
w_all_by_moved_at_full = Window.orderBy(F.col("moved_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_last_moved_at = Window.orderBy(F.col("last_moved_at").desc(), "id", "move_id")
w_move_operator_operator = Window.partitionBy("move_operator", "operator")
w_all_by_last_moved_at_ties = Window.orderBy(F.col("last_moved_at").desc())

filtered = (
    df_stock_moves
    .filter(F.col("sku_id").isNotNull())
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
        F.date_trunc("hour", F.col("moved_at")).alias("moved_at_hour"),
    )
)

windowed = (
    filtered
    .select(
        "*",
        F.max("qty").over(w_sku_id_warehouse_id_by_moved).alias("qty_max_3_prior"),
        F.round(F.avg("qty").over(w_operator_warehouse_id), 2).alias("operator_warehouse_id_avg_qty"),
        F.ntile(5).over(w_all_by_moved_at).alias("moved_at_quintile"),
        F.last("moved_at").over(w_all_by_moved_at_full).alias("last_moved_at"),
    )
)

detail_rows = (
    windowed.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        "s.moved_at",
        "s.operator",
        "s.qty_max_3_prior",
        "s.operator_warehouse_id_avg_qty",
        "s.last_moved_at",
        F.col("st.id").alias("move_id"),
        F.col("st.operator").alias("move_operator"),
        F.col("st.unit_price").alias("move_unit_price"),
    )
)

enriched_sku = (
    detail_rows.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select("s.*", F.col("sk.weight_kg").alias("sku_weight_kg"))
)

sequenced = (
    enriched_sku
    .select(
        "id",
        "sku_id",
        "qty",
        "operator",
        "qty_max_3_prior",
        "operator_warehouse_id_avg_qty",
        "move_id",
        "move_operator",
        "sku_weight_kg",
        F.round(F.percent_rank().over(w_all_by_last_moved_at), 3).alias("last_moved_at_percent_rank"),
        F.size(F.collect_set("warehouse_id").over(w_move_operator_operator)).alias("distinct_warehouse_id_in_move_operator_operator"),
        F.ntile(3).over(w_all_by_last_moved_at).alias("last_moved_at_tercile"),
        F.dense_rank().over(w_all_by_last_moved_at_ties).alias("last_moved_at_dense_rank"),
        F.lead("last_moved_at").over(w_all_by_last_moved_at).alias("next_last_moved_at"),
    )
)

scored = (
    sequenced
    .filter(F.col("move_id") <= 59)
    .select(
        "*",
        F.lower(F.regexp_replace(F.trim(F.col("operator")), " ", "_")).alias("operator_slug"),
        F.date_format(F.col("next_last_moved_at"), "yyyy-MM-dd HH:00").alias("next_last_moved_at_label"),
        F.round(F.col("qty") / F.nullif(F.col("sku_weight_kg"), F.lit(0)), 3).alias("qty_per_sku_weight_kg"),
    )
)

result = (
    scored
    .select(
        "id",
        "sku_id",
        "qty",
        "operator",
        "qty_max_3_prior",
        "operator_warehouse_id_avg_qty",
        "move_id",
        "move_operator",
        "sku_weight_kg",
        "last_moved_at_percent_rank",
        "distinct_warehouse_id_in_move_operator_operator",
        "last_moved_at_tercile",
        "last_moved_at_dense_rank",
        "next_last_moved_at",
        "operator_slug",
        "next_last_moved_at_label",
        "qty_per_sku_weight_kg",
    )
    .orderBy(
        F.col("distinct_warehouse_id_in_move_operator_operator").desc_nulls_first(),
    )
)
