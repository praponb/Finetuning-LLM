from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(
        F.when(F.col("revenue") >= 3900, "high").when(F.col("revenue") >= 1300, "mid").otherwise("low").alias("revenue_bucket"),
        F.quarter("sale_date").alias("sale_quarter"),
    )
    .agg(F.count("revenue").alias("revenue_count"))
    .filter(F.col("revenue_count") < 4)
    .orderBy(F.col("sale_quarter").desc())
)
