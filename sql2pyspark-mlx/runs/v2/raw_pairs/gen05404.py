from pyspark.sql import functions as F

base = df_suppliers.filter(F.col("country").isin(["TH", "UK", "JP"]))

agg = (
    base
    .groupBy(
        F.when(F.col("rating") >= 3.6, "high").when(F.col("rating") >= 2.3, "mid").otherwise("low").alias("rating_bucket"),
        "country",
    )
    .agg(
        F.round(F.stddev("rating"), 2).alias("stddev_rating"),
        F.sum(F.when(F.col("country") == "TH", F.col("rating")).otherwise(0)).alias("th_rating"),
    )
)

result = (
    agg
    .filter(F.col("stddev_rating") >= 59)
    .select("rating_bucket", "country", "stddev_rating", "th_rating")
    .orderBy("stddev_rating")
)
