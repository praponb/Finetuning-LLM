from pyspark.sql import functions as F

lines = (
    df_carts
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.explode("attributes").alias("key", "attr_value"),
    )
)

exploded = (
    lines
    .select(
        "id",
        "customer_id",
        "key",
        F.explode_outer("coupon_codes").alias("element"),
    )
)

base = (
    exploded
    .filter(F.col("key").isNotNull())
    .withColumn("element_key", F.concat("element", F.lit(" "), "key"))
    .withColumn("resolved_key", F.coalesce("key", F.lit("UNKNOWN")))
)

enriched = (
    base
    .alias("a")
    .join(df_customers.alias("b"), F.col("a.customer_id") == F.col("b.id"), "left")
    .filter(F.col("b.age") < 30)
    .select("a.*", F.col("b.name"), F.col("b.email"), F.col("b.segment"))
)

base_carts = (
    enriched
    .filter(F.col("customer_id") == 25)
    .select(
        "id",
        "customer_id",
        "key",
        "element",
        "element_key",
        "name",
        "email",
        "segment",
        F.concat("name", F.lit(", "), "key").alias("name_key"),
        F.coalesce("key", F.lit("NONE")).alias("resolved_key_2"),
    )
)

result = (
    base_carts
    .groupBy("element")
    .agg(
        F.count(F.when(F.col("element") == "WELCOME5", 1)).alias("welcome5_count"),
        F.sort_array(F.collect_set("segment")).alias("segment_list"),
        F.sort_array(F.collect_set("key")).alias("key_list"),
        F.count_if(F.col("element") == "SPRING10").alias("spring10_events"),
    )
)
