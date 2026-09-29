from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country").isin(["US", "TH", "UK"]) & ((F.col("country") == "UK") | F.col("city").isin(["New York", "Bangkok"])))
    .groupBy(
        F.when(F.col("rating") >= 3.6, "high").when(F.col("rating") >= 1.7, "mid").otherwise("low").alias("rating_bucket"),
        F.when(F.col("city").isin(["Chiang Mai", "Bangkok"]), "group_a").otherwise("other").alias("city_group"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("country") == "US", 1).otherwise(0))) / F.count("*")), 1).alias("pct_us"),
        (F.max("rating") - F.min("rating")).alias("rating_range"),
    )
    .orderBy(F.col("pct_us").desc(), "rating_bucket", "city_group")
    .limit(3)
)
