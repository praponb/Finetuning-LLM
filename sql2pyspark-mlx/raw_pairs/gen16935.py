from pyspark.sql import functions as F

matched_keys = (
    df_customers
    .select("city", "country")
    .subtract(
        df_customers
        .filter(F.col("city").isin(["London", "Chiang Mai", "Singapore"]))
        .select("city", "country")
    )
)

kept_rows = (
    df_customers
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.city") == F.col("k.city")) & (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy(F.when(F.col("age") >= 63, "HIGH").otherwise("LOW").alias("age_tier"))
    .agg(
        F.round(F.sum("age") / F.nullif(F.count(F.when(F.col("country") == "DE", 1)), F.lit(0)), 2).alias("age_per_de"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
    )
)
