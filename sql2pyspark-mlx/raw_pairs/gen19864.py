from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("age") <= 40) & F.col("city").isin(["Chiang Mai", "New York"]))
    .groupBy(
        F.when(F.col("segment").isin(["Consumer", "SMB"]), "priority").otherwise("other").alias("segment_group"),
    )
    .agg((F.max("credit_limit") - F.min("credit_limit")).alias("credit_limit_range"))
    .orderBy(F.col("credit_limit_range").desc())
)
