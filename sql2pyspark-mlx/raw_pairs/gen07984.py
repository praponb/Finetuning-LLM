from pyspark.sql import functions as F

lines = df_carts.select("id", F.explode("coupon_codes").alias("code"))

valid = (
    lines
    .filter(F.col("code").isNotNull())
    .withColumn("code_group", F.when(F.col("code").isin(["VIP", "SPRING10"]), "Priority").otherwise("Other"))
)

cleaned = (
    valid
    .filter(F.col("code").isin(["BUNDLE", "WELCOME5", "VIP"]))
    .select(
        "*",
        F.substring("code", 1, 3).alias("code_prefix"),
        F.coalesce("code", F.lit("NONE")).alias("code_clean"),
    )
)

cleaned_carts = (
    cleaned
    .filter(~F.col("code").isin(["SPRING10", "WELCOME5"]))
    .select(
        "id",
        "code_group",
        "code_prefix",
        "code_clean",
        F.lower("code").alias("code_lower"),
        F.when(F.col("code") == "WELCOME5", 1).otherwise(0).alias("is_welcome5"),
        F.coalesce("code", F.lit("OTHER")).alias("code_filled"),
    )
)

result = (
    cleaned_carts
    .select("id", "code_group", "code_lower", "is_welcome5", "code_filled")
    .orderBy("is_welcome5")
)
