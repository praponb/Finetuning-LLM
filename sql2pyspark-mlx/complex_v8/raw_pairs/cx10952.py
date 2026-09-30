from pyspark.sql import functions as F

valid_moves = (
    df_stock_moves
    .filter(F.col("qty") > 99)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        "unit_price",
        F.abs(F.col("unit_price") - 272.4).alias("unit_price_distance"),
        F.concat(F.col("operator"), F.lit("#"), F.col("id").cast("string")).alias("operator_ref"),
    )
)

with_parent = (
    valid_moves.alias("s")
    .join(df_warehouses.alias("w"), (F.col("w.id") == F.col("s.warehouse_id")) & (F.lower(F.col("w.wh_code")).like("%cn%")), "inner")
    .filter(F.col("w.opened_on").isNotNull())
    .select(
        "s.*",
        F.col("w.city").alias("warehouse_city"),
        F.col("w.opened_on").alias("warehouse_opened_on"),
    )
)

enriched_warehouse = (
    with_parent.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.operator",
        "s.unit_price",
        "s.unit_price_distance",
        "s.operator_ref",
        "s.warehouse_city",
        "s.warehouse_opened_on",
        F.col("w.wh_code").alias("warehouse_wh_code"),
        F.col("w.capacity").alias("warehouse_capacity"),
    )
)

per_group = (
    enriched_warehouse
    .filter(F.col("qty") >= 111)
    .groupBy(
        F.date_trunc("month", "warehouse_opened_on").alias("warehouse_opened_on_month"),
        "sku_id",
    )
    .agg(
        F.sum(F.when(F.col("warehouse_city") == "Bangkok", F.col("unit_price"))).alias("unit_price_warehouse_city_bangkok"),
        F.array_sort(F.collect_set("move_type")).alias("move_type_list"),
        F.array_sort(F.collect_set("warehouse_wh_code")).alias("warehouse_wh_code_list"),
        F.datediff(F.max("warehouse_opened_on"), F.min("warehouse_opened_on")).alias("warehouse_opened_on_span_days"),
        F.max("unit_price").alias("max_unit_price_check"),
    )
    .filter(F.col("max_unit_price_check") > 124.1)
    .drop("max_unit_price_check")
)

result = (
    per_group
    .select(
        "warehouse_opened_on_month",
        "unit_price_warehouse_city_bangkok",
        "move_type_list",
        "warehouse_wh_code_list",
        F.array_join(F.array_sort(F.col("warehouse_wh_code_list")), ",").alias("warehouse_wh_code_list_text"),
        F.size(F.col("warehouse_wh_code_list")).alias("warehouse_wh_code_list_count"),
    )
    .orderBy(
        "warehouse_wh_code_list_text",
        F.col("unit_price_warehouse_city_bangkok").asc_nulls_last(),
    )
)
