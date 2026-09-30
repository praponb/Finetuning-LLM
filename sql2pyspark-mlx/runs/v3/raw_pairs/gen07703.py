from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(F.when(F.col("country").isin(["SG", "UK"]), "tier_1").otherwise("other").alias("country_group"))
    .pivot("segment", ["SMB", "Consumer", "Corporate"])
    .agg(F.count("age"))
)
