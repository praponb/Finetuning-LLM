from pyspark.sql import functions as F

lines = df_carts.select("id", F.explode("coupon_codes").alias("tag"))

filtered = (
    lines
    .filter(F.col("tag").isNotNull() & ~F.col("tag").isin(["BUNDLE", "VIP", "WELCOME5"]))
    .withColumn("tag_title", F.initcap(F.trim("tag")))
)

cleaned = (
    filtered
    .filter(F.col("tag").isin(["BUNDLE", "VIP", "WELCOME5"]))
    .withColumn("tag_length", F.length("tag"))
    .withColumn("tag_clean", F.coalesce("tag", F.lit("NONE")))
)

valid = (
    cleaned
    .filter(F.col("tag").isNotNull())
    .select(
        "id",
        "tag",
        "tag_title",
        "tag_length",
        F.floor("tag_length").alias("tag_length_whole"),
    )
)

base = (
    valid
    .filter(F.col("tag").isNotNull())
    .select(
        "*",
        F.when(F.col("tag_length") >= 67, "High").when(F.col("tag_length") >= 37, "Medium").otherwise("Low").alias("tag_length_band"),
    )
)

filtered_carts = (
    base
    .filter(F.col("tag") == "WELCOME5")
    .select(
        "id",
        "tag",
        "tag_length",
        "tag_length_whole",
        "tag_length_band",
        F.col("tag_length_whole").cast("string").alias("tag_length_whole_str"),
        (F.col("tag_length") * F.col("tag_length_whole")).alias("tag_length_times_tag_length_whole"),
    )
)

totals = (
    filtered_carts
    .groupBy(
        F.when(F.col("tag_length_times_tag_length_whole") >= 83, "LARGE").otherwise("SMALL").alias("tag_length_times_tag_length_whole_tier"),
    )
    .agg(
        F.percentile_approx("tag_length", 0.95).alias("tag_length_p95"),
        F.avg(F.when(F.col("tag") == "SPRING10", F.col("tag_length"))).alias("spring10_avg_tag_length"),
        F.percentile_approx("tag_length_times_tag_length_whole", 0.9).alias("tag_length_times_tag_length_whole_p90"),
        F.count("*").alias("cnt"),
        F.size(F.collect_set("tag")).alias("num_tags"),
    )
)

result = (
    totals
    .select(
        "tag_length_times_tag_length_whole_tier",
        "tag_length_p95",
        "spring10_avg_tag_length",
        "tag_length_times_tag_length_whole_p90",
        "cnt",
        "num_tags",
    )
)
