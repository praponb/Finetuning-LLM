from pyspark.sql import functions as F

lines = (
    df_carts
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.explode_outer("attributes").alias("k", "v"),
    )
)

exploded = lines.select("id", "k", "v", F.explode_outer("coupon_codes").alias("element"))

filtered = (
    exploded
    .filter(~F.col("element").isin(["BUNDLE", "SPRING10"]))
    .withColumn("element_lower", F.lower("element"))
)

cleaned = (
    filtered
    .filter(F.col("k").isNotNull())
    .select(
        "*",
        F.when(F.col("k").isin(["device", "gift"]), "Group A").otherwise("Other").alias("k_group"),
        F.length("k").alias("k_length"),
        F.coalesce("element", F.lit("DIRECT")).alias("element_clean"),
    )
)

remaining_keys = (
    cleaned
    .select("id")
    .intersectAll(
        df_carts
        .filter(F.col("customer_id").isNull())
        .select("id")
    )
)

verified = (
    cleaned
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy("element", "k")
    .agg(
        F.countDistinct("element").alias("distinct_elements"),
        F.percentile_approx("k_length", 0.9).alias("k_length_p90"),
        F.count("*").alias("num_rows"),
    )
    .orderBy(F.col("k").desc_nulls_last())
)
