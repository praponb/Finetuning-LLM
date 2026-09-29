from pyspark.sql import functions as F

base = (
    df_stock_moves
    .filter((F.col("moved_at") < F.lit("2025-02-24 12:00:00").cast("timestamp")) & (F.col("operator").isNotNull()))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        (F.col("unit_price") - F.col("qty")).alias("unit_price_minus_qty"),
        F.when(F.col("qty") >= 372, "large").when(F.col("qty") >= 116, "medium").otherwise("small").alias("qty_tier"),
    )
)

combined_details_warehouse = (
    base.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.moved_at",
        "s.unit_price",
        "s.unit_price_minus_qty",
        "s.qty_tier",
        F.col("w.opened_on").alias("warehouse_opened_on"),
        F.col("w.capacity").alias("warehouse_capacity"),
        F.col("w.city").alias("warehouse_city"),
    )
)

annotated = (
    combined_details_warehouse
    .filter(F.col("warehouse_id") >= 2)
    .select(
        "*",
        F.round(F.col("unit_price") * 1.5, 2).alias("unit_price_adj"),
        F.concat(F.col("move_type"), F.lit("#"), F.col("warehouse_capacity").cast("string")).alias("move_type_ref"),
        F.lower(F.regexp_replace(F.trim(F.col("warehouse_city")), " ", "_")).alias("warehouse_city_slug"),
    )
)

per_group = (
    annotated
    .groupBy(
        "qty_tier",
        F.year("warehouse_opened_on").alias("warehouse_opened_on_year"),
        "sku_id",
    )
    .agg(
        F.max("moved_at").alias("last_moved_at"),
        F.count(F.when(F.col("warehouse_city").isin("Bangkok", "Singapore"), 1)).alias("n_warehouse_city_in_bangkok_si"),
        F.count("*").alias("n_rows"),
        F.countDistinct("warehouse_id").alias("distinct_warehouse_ids"),
    )
)

result = (
    per_group
    .select(
        "qty_tier",
        "warehouse_opened_on_year",
        "n_warehouse_city_in_bangkok_si",
        "n_rows",
    )
    .unpivot(
        ids=["qty_tier", "warehouse_opened_on_year"],
        values=["n_warehouse_city_in_bangkok_si", "n_rows"],
        variableColumnName="field",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
    .orderBy("qty_tier", "warehouse_opened_on_year", "field")
)
