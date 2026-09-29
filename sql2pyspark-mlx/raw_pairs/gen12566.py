from pyspark.sql import functions as F

lines = (
    df_carts
    .select("id", "customer_id", "created_at", F.explode("coupon_codes").alias("code"))
)

with_dims = (
    lines
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .select("a.customer_id", "a.code", F.col("c.segment"))
)

joined = (
    with_dims
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.country"))
)

filtered = (
    joined
    .filter(F.col("customer_id").isNotNull())
    .withColumn("country_upper", F.upper("country"))
    .withColumn("country_code", F.concat("country", F.lit(" - "), "code"))
)

joined_carts = (
    filtered
    .alias("src")
    .join(df_customers.alias("b"), F.col("src.customer_id") == F.col("b.id"), "left")
    .select("src.*", F.col("b.segment").alias("customer_segment"), F.col("b.email"))
)

result = (
    joined_carts
    .rollup("country", "customer_segment")
    .agg(
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.count_if(F.col("country") == "SG").alias("sg_events"),
        F.count_if(F.col("country") == "JP").alias("jp_events"),
        F.count("*").alias("event_count"),
    )
    .filter(F.col("sg_events") < 1000)
    .orderBy(F.col("country").asc_nulls_first())
)
