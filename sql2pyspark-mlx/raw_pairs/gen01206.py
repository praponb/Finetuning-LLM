from pyspark.sql import functions as F

lines = (
    df_carts
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.explode("attributes").alias("attr_key", "attr_value"),
    )
)

joined = (
    lines
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id").eqNullSafe(F.col("c.id")), "left")
    .filter(F.col("c.age").between(41, 46))
    .select(
        "a.id",
        "a.customer_id",
        "a.attr_key",
        "a.attr_value",
        F.col("c.name"),
        F.col("c.city"),
        F.col("c.segment"),
    )
)

cleaned = (
    joined
    .filter((~F.col("attr_key").eqNullSafe(F.lit("gift"))) & F.col("customer_id").isin([24, 33]))
    .withColumn("segment_attr_key", F.concat("segment", F.lit(", "), "attr_key"))
    .withColumn("attr_key_lower", F.lower("attr_key"))
    .withColumn("city_clean", F.coalesce("city", F.lit("OTHER")))
)

matched_keys = (
    cleaned
    .select("customer_id", "id")
    .subtract(
        df_carts
        .filter(F.col("customer_id") == 15)
        .select("customer_id", "id")
    )
)

reconciled = (
    cleaned
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy("segment")
    .agg(
        F.count(F.when(F.col("city") == "Berlin", 1)).alias("berlin_rows"),
        F.count("*").alias("row_count"),
        F.sort_array(F.collect_set("attr_key")).alias("attr_key_list"),
        F.countDistinct("city").alias("distinct_cities"),
    )
    .orderBy("row_count")
)
