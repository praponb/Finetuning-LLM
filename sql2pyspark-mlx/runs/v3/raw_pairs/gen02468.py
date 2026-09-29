from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(F.when(F.col("country").isin(["JP", "UK"]), "tier_1").otherwise("other").alias("country_group"))
    .pivot("city", ["Berlin", "Chiang Mai", "Bangkok", "Singapore"])
    .agg(F.sum("credit_limit"))
)
