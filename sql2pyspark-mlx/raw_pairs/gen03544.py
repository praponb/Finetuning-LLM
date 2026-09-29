from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(
        "country",
        F.when(F.col("segment").isin(["SMB", "Consumer"]), "tier_1").otherwise("other").alias("segment_group"),
    )
    .agg(F.count("credit_limit").alias("credit_limit_count"))
    .filter(F.col("credit_limit_count") >= 5)
    .select("country", "segment_group", "credit_limit_count")
    .orderBy(F.col("segment_group").desc())
)
