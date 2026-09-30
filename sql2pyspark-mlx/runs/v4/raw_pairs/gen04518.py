from pyspark.sql import functions as F

matched_keys = (
    df_customers
    .select("city", "id")
    .subtract(
        df_customers
        .filter(F.lower("name").like("%vy %"))
        .select("city", "id")
    )
)

verified = (
    df_customers
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.city") == F.col("k.city")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .withColumn("age_tier", F.when(F.col("age") >= 61, "LARGE").otherwise("SMALL"))
    .withColumn("credit_limit_tier", F.when(F.col("credit_limit") >= 18000, "HIGH").otherwise("LOW"))
    .rollup("age_tier", "segment", "credit_limit_tier")
    .agg(
        F.sum(F.when(F.col("city") == "Berlin", F.col("age"))).alias("berlin_total_age"),
        F.sort_array(F.collect_set("segment")).alias("segment_list"),
        F.max(F.when(F.col("city") == "London", F.col("credit_limit"))).alias("london_max_credit_limit"),
        F.count(F.when(F.col("country") == "DE", 1)).alias("de_rows"),
        F.grouping_id().alias("grouping_level"),
    )
    .filter(F.col("berlin_total_age") < 0)
)
