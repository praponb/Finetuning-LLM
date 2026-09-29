from pyspark.sql import functions as F

exploded = (
    df_carts
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.explode_outer("attributes").alias("key", "attr_value"),
    )
)

flattened = exploded.select("id", "created_at", F.explode_outer("coupon_codes").alias("code"))

filtered = (
    flattened
    .filter(F.col("code").isNotNull())
    .withColumn("created_at_period", F.date_trunc("MONTH", "created_at"))
)

filtered_carts = (
    filtered
    .filter((F.col("created_at") >= F.to_date(F.lit("2025-03-01"))) & F.col("code").isNotNull())
    .withColumn("code_length", F.length("code"))
    .withColumn("resolved_code", F.coalesce("code", F.lit("DIRECT")))
)

base = (
    filtered_carts
    .filter(F.col("code").isNotNull() & (~F.col("code").eqNullSafe(F.lit("VIP"))))
    .select(
        "created_at",
        "code",
        "created_at_period",
        "code_length",
        "resolved_code",
        F.floor("code_length").alias("code_length_whole"),
    )
)

result = (
    base
    .select(
        "created_at",
        "code",
        "created_at_period",
        "code_length",
        "resolved_code",
        "code_length_whole",
        F.when(F.col("code").rlike("^(WELCOME5|VIP)$"), "EXTENDED").when(F.col("code").rlike("^(SPRING10|BUNDLE)$"), "GROUP_A").otherwise("CORE").alias("code_zone"),
    )
    .orderBy(F.col("code_length_whole").desc())
)
