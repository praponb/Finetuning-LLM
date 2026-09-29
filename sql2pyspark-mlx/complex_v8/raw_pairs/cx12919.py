from pyspark.sql import functions as F

cleaned = (
    df_stock_moves
    .filter(F.col("sku_id").isNotNull())
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "operator",
        "unit_price",
        F.when(F.col("operator").isin("op_dew", "op_chan"), "group_a").when(F.col("operator") == "op_bee", "group_b").otherwise("other").alias("operator_group"),
        F.concat_ws(" / ", F.col("operator"), F.col("move_type")).alias("operator_move_type_label"),
        F.coalesce(F.col("move_type"), F.lit("UNKNOWN")).alias("move_type_or_unknown"),
    )
)

summary = (
    cleaned
    .groupBy("move_type_or_unknown", "warehouse_id")
    .agg(
        F.array_sort(F.collect_set("operator")).alias("operator_list"),
        F.sum("qty").alias("total_qty"),
    )
)

with_details = (
    summary.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.*",
        F.col("w.opened_on").alias("warehouse_opened_on"),
        F.col("w.city").alias("warehouse_city"),
    )
)

result = (
    with_details
    .filter(F.col("warehouse_opened_on") < F.lit("2025-07-03").cast("date"))
    .select(
        "move_type_or_unknown",
        "total_qty",
        "warehouse_opened_on",
        "warehouse_city",
        F.date_format(F.col("warehouse_opened_on"), "yyyy-MM").alias("warehouse_opened_on_label"),
    )
    .orderBy(F.col("total_qty").desc())
)
