from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        F.when(F.col("country").isin(["DE", "TH"]), "tier_1").otherwise("other").alias("country_group"),
        F.when(F.col("rating") >= 4.1, "high").when(F.col("rating") >= 1.6, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.sum("rating").alias("total_rating"))
)
