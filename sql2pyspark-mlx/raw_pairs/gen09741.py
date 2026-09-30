from pyspark.sql import functions as F

base = (
    df_store_sales
    .filter((F.col("revenue") <= 1800) & (F.col("sale_date") <= "2023-09-20"))
)

summary = (
    base
    .groupBy("product_id")
    .agg(
        F.count("*").alias("cnt"),
        F.count("revenue").alias("revenue_count"),
    )
)

result = (
    summary
    .filter(F.col("revenue_count") > 6)
    .orderBy(F.col("revenue_count").desc(), F.col("product_id").desc())
    .limit(1)
)
