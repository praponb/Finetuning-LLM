from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("product_name").isNotNull())
    .groupBy(F.date_trunc("MONTH", "launch_date").alias("launch_month"))
    .agg(
        F.min("launch_date").alias("min_launch_date"),
        F.count(F.when(F.col("brand") == "Acme", 1)).alias("acme_count"),
    )
    .filter(F.col("acme_count") > 9)
    .orderBy(F.col("acme_count").desc())
)
