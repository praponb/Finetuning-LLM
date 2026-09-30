from pyspark.sql import functions as F

flattened = df_carts.select("id", F.explode("coupon_codes").alias("code"))

cleaned = (
    flattened
    .filter(F.col("code").isNotNull())
    .withColumn("code_group", F.when(F.col("code").isin(["BUNDLE", "WELCOME5"]), "Core").otherwise("Other"))
    .withColumn("code_prefix", F.substring("code", 1, 4))
    .withColumn("code_filled", F.coalesce("code", F.lit("DIRECT")))
)

cleaned_carts = (
    cleaned
    .filter(F.col("code").isNotNull() & (F.col("code") != "BUNDLE"))
    .select(
        "id",
        "code",
        "code_group",
        "code_prefix",
        F.when(F.col("code").isin(["BUNDLE", "WELCOME5"]), "Group A").otherwise("Other").alias("code_group_2"),
    )
)

result = (
    cleaned_carts
    .groupBy("code")
    .agg(
        F.count(F.when(F.col("code") == "WELCOME5", 1)).alias("welcome5_rows"),
        F.count(F.when(F.col("code") == "BUNDLE", 1)).alias("bundle_count"),
    )
    .orderBy(F.col("welcome5_rows").desc_nulls_last())
)
