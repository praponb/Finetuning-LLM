from pyspark.sql import functions as F

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode("attributes").alias("key", "v"),
    )
)

flattened = (
    exploded
    .select(
        "id",
        "created_at",
        "key",
        "v",
        F.explode_outer("coupon_codes").alias("code"),
    )
)

valid = (
    flattened
    .filter(F.col("created_at") >= F.lit("2025-03-02 09:00:00").cast("timestamp"))
    .withColumn("is_gift", F.when(F.col("key") == "gift", 1).otherwise(0))
    .withColumn("key_clean", F.coalesce("key", F.lit("DIRECT")))
)

matched_keys = (
    valid
    .select("id")
    .intersect(
        df_carts
        .filter(F.col("customer_id").isNull())
        .select("id")
    )
)

verified = (
    valid
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .select(
        "id",
        "created_at",
        "code",
        F.when(F.col("code").rlike("^(WELCOME5|SPRING10)$"), "PRIORITY").when(F.col("code").rlike("^(VIP|BUNDLE)$"), "GROUP_B").otherwise("EXTENDED").alias("code_zone"),
    )
    .orderBy(F.col("id").desc_nulls_first(), F.col("code_zone").desc())
)
