from pyspark.sql import functions as F

src = (
    df_suppliers
    .filter(~F.col("city").isin(["New York", "Bangkok"]) & (F.col("city").isNull() | (F.col("city") != "Bangkok")))
)

agg = (
    src
    .groupBy(
        F.when(F.col("rating") >= 3.7, "high").when(F.col("rating") >= 1.6, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("country") == "US", 1).otherwise(0))) / F.count("*")), 1).alias("pct_us"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
    )
)

result = (
    agg
    .filter(F.col("pct_us") >= 56)
    .select("rating_bucket", "pct_us")
    .orderBy(F.col("rating_bucket").desc(), F.col("pct_us").desc())
)
