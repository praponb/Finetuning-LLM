from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("carrier") == "Kerry")
    .groupBy(
        F.when(F.col("cost") >= 82, "high").when(F.col("cost") >= 28, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(F.count("*").alias("row_count"))
)
