from pyspark.sql import functions as F

lines = df_carts.select("id", "created_at", F.explode_outer("coupon_codes").alias("code"))

base = (
    lines
    .filter((F.col("created_at") >= F.lit("2025-03-01 19:00:00").cast("timestamp")) & F.col("code").isNotNull())
    .select(
        "*",
        F.datediff(F.current_date(), "created_at").alias("days_since_created_at"),
    )
)

filtered = (
    base
    .filter(F.col("code").isin(["WELCOME5", "BUNDLE"]))
    .withColumn("created_at_weekofyear", F.weekofyear("created_at"))
    .withColumn("days_since_created_at_whole", F.floor("days_since_created_at"))
    .withColumn("resolved_code", F.coalesce("code", F.lit("NONE")))
)

filtered_carts = (
    filtered
    .filter((F.col("code").eqNullSafe(F.lit("WELCOME5"))) & F.col("code").isNotNull())
    .select(
        "id",
        "days_since_created_at",
        "created_at_weekofyear",
        "days_since_created_at_whole",
        F.greatest("days_since_created_at", "created_at_weekofyear").alias("greatest_days_since_created_at_created_at_weekofyear"),
        F.coalesce("code", F.lit("UNKNOWN")).alias("resolved_code_2"),
    )
)

result = (
    filtered_carts
    .select(
        "id",
        "days_since_created_at",
        "created_at_weekofyear",
        "greatest_days_since_created_at_created_at_weekofyear",
        "resolved_code_2",
    )
    .orderBy(F.col("id").desc(), "days_since_created_at")
)
