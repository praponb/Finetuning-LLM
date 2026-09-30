from pyspark.sql import functions as F

src = (
    df_store_sales
    .filter(F.col("product_id").isin([2, 5, 6]) & F.col("sale_date").between("2024-05-22", "2025-04-16"))
    .withColumn("revenue_per_unit", (F.col("revenue") * F.col("units")))
)

store_id_summary = (
    src
    .groupBy("store_id")
    .agg(
        F.sum("revenue_per_unit").alias("total_revenue_per_unit"),
        F.max("revenue_per_unit").alias("max_revenue_per_unit"),
    )
)

result = (
    store_id_summary
    .filter(F.col("max_revenue_per_unit") < 44)
    .select("store_id", "total_revenue_per_unit", "max_revenue_per_unit")
    .orderBy(F.col("store_id").desc())
)
