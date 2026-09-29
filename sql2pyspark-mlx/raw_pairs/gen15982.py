from pyspark.sql import functions as F

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.explode_outer("attributes").alias("attr_key", "v"),
    )
)

lines = exploded.select("id", "customer_id", "v", F.explode("coupon_codes").alias("tag"))

base = (
    lines
    .filter((F.col("tag").eqNullSafe(F.lit("WELCOME5"))) & F.col("customer_id").isNotNull())
    .withColumn("clean_tag", F.trim("tag"))
    .withColumn("tag_clean", F.coalesce("tag", F.lit("OTHER")))
)

valid = (
    base
    .filter(F.col("tag").isNotNull())
    .select(
        "id",
        "customer_id",
        "v",
        "tag",
        "clean_tag",
        "tag_clean",
        F.when(F.col("tag") == "SPRING10", 1).otherwise(0).alias("is_spring10"),
        F.coalesce("tag", F.lit("OTHER")).alias("resolved_tag"),
    )
)

joined = (
    valid
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id").eqNullSafe(F.col("c.id")), "left")
    .filter(F.col("c.city").isin(["Tokyo", "Singapore", "New York"]))
    .select("a.*", F.col("c.email"))
)

result = (
    joined
    .cube("tag")
    .agg(
        F.min("is_spring10").alias("min_is_spring10"),
        F.avg(F.when(F.col("tag") == "WELCOME5", F.col("is_spring10"))).alias("welcome5_avg_is_spring10"),
        F.grouping("tag").alias("grouping_id"),
    )
)
