from pyspark.sql import functions as F

src = df_suppliers.filter(F.col("city").isin(["Bangkok", "Berlin", "Chiang Mai"]))

agg = (
    src
    .groupBy(
        F.when(F.col("country").isin(["SG", "TH"]), "group_a").otherwise("other").alias("country_group"),
    )
    .agg(
        F.sum(F.when(F.col("country") == "TH", F.col("rating")).otherwise(0)).alias("th_rating"),
        F.count(F.when(F.col("city") == "Berlin", 1)).alias("berlin_count"),
    )
)

result = (
    agg
    .filter(F.col("berlin_count") > 9)
    .select("country_group", "th_rating", "berlin_count")
    .orderBy(F.col("th_rating").desc(), F.col("berlin_count").desc(), "country_group")
    .limit(1)
)
