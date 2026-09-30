from pyspark.sql import functions as F

recent_suppliers = (
    df_suppliers
    .filter(F.col("country").isin(["DE", "UK", "TH"]))
    .withColumn("country_prefix", F.substring("country", 1, 4))
)

agg = (
    recent_suppliers
    .groupBy(
        F.when(F.col("rating") >= 4.1, "high").when(F.col("rating") >= 1.4, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.min("rating").alias("min_rating"))
)

result = agg.filter(F.col("min_rating") < 4.0)
