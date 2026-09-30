from pyspark.sql import functions as F

valid_suppliers = (
    df_suppliers
    .filter((F.col("rating") <= 1.9) & ((F.col("country") == "US") | F.col("country").isin(["UK", "TH", "JP"])))
    .withColumn("city_country_key", F.concat_ws("-", "city", "country"))
)

agg = (
    valid_suppliers
    .groupBy("city")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("city") == "London", 1).otherwise(0))) / F.count("*")), 1).alias("pct_london"),
        F.countDistinct("rating").alias("unique_countrys"),
    )
)

result = (
    agg
    .select("city", "pct_london", "unique_countrys")
    .orderBy("pct_london")
)
