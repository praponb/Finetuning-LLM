from pyspark.sql import functions as F

filtered = (
    df_products
    .filter((F.col("launch_date") >= F.to_date(F.lit("2023-03-02"))) & (~F.col("brand").eqNullSafe(F.lit("Stark"))))
    .select(
        "id",
        "product_name",
        "brand",
        "cost",
        "stock_qty",
        "launch_date",
        "supplier_id",
        F.when(F.col("cost") >= 590, "High").when(F.col("cost") >= 260, "Medium").otherwise("Low").alias("cost_band"),
        F.least("stock_qty", "cost").alias("least_stock_qty_cost"),
        F.coalesce("brand", F.lit("UNKNOWN")).alias("brand_filled"),
    )
)

result = (
    filtered
    .alias("s")
    .join(df_suppliers.alias("d"), F.col("s.supplier_id") == F.col("d.id"), "inner")
    .rollup(
        F.col("d.country").alias("country"),
        F.col("d.city").alias("city"),
        F.col("s.brand").alias("brand"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.avg(F.col("s.cost")).alias("avg_cost"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy(F.col("rows_total").asc_nulls_first())
)
