from pyspark.sql import functions as F

prepared = (
    df_stock_moves
    .filter((F.col("operator") == "op_eve") & (F.col("warehouse_id") <= 5))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.least(F.col("unit_price"), F.col("qty")).alias("least_unit_price_qty"),
        F.coalesce(F.col("unit_price"), F.lit(0.0)).alias("unit_price_filled"),
        F.concat_ws("-", F.col("operator"), F.col("move_type")).alias("operator_move_type_label"),
    )
)

with_parent = (
    prepared.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.*",
        "sk.sku_code",
        F.col("sk.labels").alias("sku_labels"),
        F.col("sk.is_active").alias("sku_is_active"),
        F.col("w.wh_code").alias("warehouse_wh_code"),
        F.col("w.city").alias("warehouse_city"),
    )
)

grouped = (
    with_parent
    .groupBy("operator", "sku_id", "sku_is_active")
    .agg(
        F.max("moved_at").alias("last_moved_at"),
        F.round(F.avg("qty"), 2).alias("avg_qty"),
        F.count("*").alias("n_rows"),
        F.round(F.sum("least_unit_price_qty"), 2).alias("total_least_unit_price_qty"),
    )
)

labeled = (
    grouped
    .filter(F.col("sku_is_active"))
    .select(
        "*",
        F.coalesce(F.col("operator"), F.lit("UNKNOWN")).alias("operator_or_unknown"),
        (F.floor(F.col("avg_qty") / 100) * 100).alias("avg_qty_bucket"),
        F.when(F.col("operator").isin("op_chan", "op_ann"), "group_a").when(F.col("operator") == "op_dew", "group_b").otherwise("other").alias("operator_group"),
    )
)

result = (
    labeled
    .select(
        "operator",
        "sku_id",
        "last_moved_at",
        "n_rows",
        "total_least_unit_price_qty",
        "avg_qty_bucket",
        "operator_group",
    )
    .orderBy(F.col("n_rows").desc(), F.col("total_least_unit_price_qty").desc())
)
