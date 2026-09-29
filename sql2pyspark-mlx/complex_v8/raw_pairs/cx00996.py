from pyspark.sql import functions as F

base = (
    df_stock_moves
    .filter(F.col("unit_price") < 361.9)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "unit_price",
        F.when(F.col("operator").isin("op_dew", "op_ann"), "group_a").when(F.col("operator") == "op_eve", "group_b").otherwise("other").alias("operator_group"),
        F.greatest(F.col("qty"), F.col("unit_price")).alias("greatest_qty_unit_price"),
    )
)

totals = (
    base
    .filter(F.col("move_type").isin("ADJUST", "INBOUND"))
    .groupBy(
        F.date_trunc("month", "moved_at").alias("moved_at_month"),
        "warehouse_id",
        F.when(F.col("qty") >= 300, "HIGH").when(F.col("qty") >= 111, "MEDIUM").otherwise("LOW").alias("qty_tier"),
    )
    .agg(
        F.max("greatest_qty_unit_price").alias("max_greatest_qty_unit_price"),
        F.round(F.sum("greatest_qty_unit_price"), 2).alias("total_greatest_qty_unit_price"),
        F.max("moved_at").alias("last_moved_at"),
        F.round(100 * F.count(F.when(F.col("move_type") != "TRANSFER", 1)) / F.count("*"), 1).alias("pct_move_type_transfer"),
        F.sum("greatest_qty_unit_price").alias("sum_greatest_qty_unit_price_check"),
    )
    .filter(F.col("sum_greatest_qty_unit_price_check") > 200.6)
    .drop("sum_greatest_qty_unit_price_check")
)

with_parent_warehouse = (
    totals.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.*",
        F.col("w.city").alias("warehouse_city"),
        F.col("w.capacity").alias("warehouse_capacity"),
        F.col("w.wh_code").alias("warehouse_wh_code"),
    )
)

annotated = (
    with_parent_warehouse
    .withColumn("minutes_moved_at_month_to_last_moved_at", F.round((F.unix_timestamp(F.col("last_moved_at")) - F.unix_timestamp(F.col("moved_at_month"))) / 60, 1))
)

result = (
    annotated
    .select(
        "moved_at_month",
        "warehouse_id",
        "total_greatest_qty_unit_price",
        "last_moved_at",
        "pct_move_type_transfer",
        "warehouse_city",
        "warehouse_wh_code",
        "minutes_moved_at_month_to_last_moved_at",
    )
    .orderBy(F.col("total_greatest_qty_unit_price").asc_nulls_last(), "warehouse_id")
)
