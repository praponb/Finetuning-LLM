from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city").isin(["London", "Berlin", "New York"]) & F.col("country").isin(["US", "DE", "SG"]))
    .groupBy(
        F.when(F.col("country").isin(["SG", "US"]), "tier_1").otherwise("other").alias("country_group"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("city") == "Chiang Mai", 1).otherwise(0))) / F.count("*")), 1).alias("pct_chiang_mai"),
        F.concat_ws(", ", F.sort_array(F.collect_set("country"))).alias("country_values"),
    )
    .orderBy("country_group")
)
