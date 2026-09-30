from pyspark.sql import functions as F

lines = df_carts.select("id", F.explode("coupon_codes").alias("tag"))

valid = (
    lines
    .filter(F.col("tag").isNotNull() & (F.col("tag") != "BUNDLE"))
    .select(
        "*",
        F.trim("tag").alias("clean_tag"),
        F.coalesce("tag", F.lit("N/A")).alias("tag_filled"),
    )
)

cleaned = (
    valid
    .filter(F.col("tag") == "BUNDLE")
    .withColumn("tag_prefix", F.substring("tag", 1, 2))
    .withColumn("tag_lower", F.lower("tag"))
    .withColumn("resolved_tag", F.coalesce("tag", F.lit("OTHER")))
)

cleaned_carts = (
    cleaned
    .filter(F.col("tag").isin(["SPRING10", "VIP"]))
    .select(
        "id",
        "tag",
        "clean_tag",
        "tag_filled",
        "tag_prefix",
        "tag_lower",
        "resolved_tag",
        F.coalesce("tag", F.lit("N/A")).alias("tag_filled_2"),
    )
)

report = (
    cleaned_carts
    .select(
        "id",
        "tag",
        "tag_filled",
        F.when(F.col("tag").rlike("^(WELCOME5|SPRING10)$"), "CORE").otherwise("OTHER_ZONE").alias("tag_zone"),
    )
)

result = (
    report
    .select("id", "tag", "tag_filled", "tag_zone")
    .orderBy("tag")
)
