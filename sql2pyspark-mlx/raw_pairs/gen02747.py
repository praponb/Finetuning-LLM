from pyspark.sql import functions as F

flattened = df_carts.select("id", F.explode("coupon_codes").alias("value"))

matched_keys = (
    flattened
    .select("id")
    .subtract(
        df_carts
        .filter(F.col("created_at") >= "2025-06-01")
        .select("id")
    )
)

kept_rows = (
    flattened
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

filtered = (
    kept_rows
    .filter(F.col("value") == "VIP")
    .withColumn("value_title", F.initcap(F.trim("value")))
)

filtered_carts = (
    filtered
    .filter(F.col("value").isNotNull() & F.col("value").isin(["WELCOME5", "SPRING10", "BUNDLE"]))
    .select(
        "*",
        F.when(F.col("value").isin(["WELCOME5", "SPRING10"]), "Core").otherwise("Other").alias("value_group"),
        F.coalesce("value", F.lit("OTHER")).alias("value_filled"),
    )
)

result = (
    filtered_carts
    .select("id", "value_title", "value_group", "value_filled")
    .orderBy(F.col("id").desc())
)
