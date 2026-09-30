from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("segment").isin(["SMB", "Corporate"]))
    .groupBy("country")
    .agg(
        F.count("*").alias("total_count"),
        F.sum("age").alias("total_age"),
    )
    .orderBy(F.col("total_count").desc())
)
