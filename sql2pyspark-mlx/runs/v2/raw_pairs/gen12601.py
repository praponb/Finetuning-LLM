from pyspark.sql import functions as F

base = (
    df_store_sales
    .filter((F.month("sale_date").isin([1, 8, 12]) & (F.col("units") != 34)) | F.col("product_id").isin([1, 2, 6, 9]))
    .withColumn("revenue_per_unit", F.round((F.col("revenue") / F.col("units")), 2))
)

summary = (
    base
    .groupBy("store_id")
    .agg(
        F.max("sale_date").alias("latest_sale_date"),
        F.min("units").alias("min_units"),
    )
)

result = (
    summary
    .select("store_id", "latest_sale_date", "min_units")
    .orderBy(F.col("min_units").desc(), "store_id")
)
