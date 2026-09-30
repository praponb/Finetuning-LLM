from pyspark.sql import functions as F

exploded = df_carts.select("id", "customer_id", F.explode("coupon_codes").alias("tag"))

with_dims = (
    exploded
    .alias("a")
    .join(df_customers.alias("b"), F.col("a.customer_id").eqNullSafe(F.col("b.id")), "inner")
    .select("a.*", F.col("b.country"))
)

filtered = (
    with_dims
    .filter(F.col("tag").isNotNull())
    .withColumn("country_filled", F.coalesce("country", F.lit("unassigned")))
    .withColumn("tag_country_key", F.concat_ws(",", "tag", "country"))
)

joined = (
    filtered
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id") == F.col("p.id"), "left")
    .select(
        "src.id",
        "src.customer_id",
        "src.tag",
        "src.country",
        "src.tag_country_key",
        F.col("p.city"),
    )
)

result = (
    joined
    .groupBy("city", "country")
    .agg(
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.sort_array(F.collect_set("city")).alias("city_list_2"),
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count(F.when(F.col("country") == "SG", 1)).alias("sg_count"),
    )
    .orderBy("distinct_customer_ids")
)
