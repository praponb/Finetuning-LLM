from pyspark.sql import functions as F

valid_store_sales = df_store_sales.filter(F.col("units").isNotNull())

summary = (
    valid_store_sales
    .groupBy("product_id")
    .agg(
        F.max("sale_date").alias("last_sale_date"),
        F.count("units").alias("units_count"),
    )
)

result = (
    summary
    .filter(F.col("units_count") >= 4)
    .orderBy(F.col("product_id").desc())
    .limit(3)
)
