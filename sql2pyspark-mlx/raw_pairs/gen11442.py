from pyspark.sql import functions as F

lines = df_carts.select("id", F.explode("coupon_codes").alias("code"))

base = (
    lines
    .filter(~F.col("code").eqNullSafe(F.lit("BUNDLE")))
    .withColumn("code_upper", F.upper("code"))
)

filtered = (
    base
    .filter(F.col("code") == "VIP")
    .select(
        "*",
        F.initcap(F.trim("code")).alias("code_title"),
        F.coalesce("code", F.lit("N/A")).alias("code_clean"),
    )
)

valid = (
    filtered
    .filter(F.col("code").isin(["WELCOME5", "SPRING10"]))
    .select(
        "id",
        "code",
        "code_upper",
        "code_clean",
        F.trim("code").alias("clean_code"),
        F.coalesce("code", F.lit("N/A")).alias("resolved_code"),
    )
)

valid_carts = (
    valid
    .filter((~F.col("code").eqNullSafe(F.lit("BUNDLE"))) & F.col("code").isNotNull())
    .withColumn("code_upper_2", F.upper("code"))
    .withColumn("code_clean_2", F.coalesce("code", F.lit("N/A")))
)

result = (
    valid_carts
    .select(
        "code",
        "code_upper",
        "code_clean",
        "clean_code",
        "code_upper_2",
        "code_clean_2",
    )
    .orderBy(F.col("code").asc_nulls_last())
)
