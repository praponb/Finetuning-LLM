from pyspark.sql import functions as F

exploded = df_carts.select("id", "created_at", F.explode("coupon_codes").alias("code"))

base = (
    exploded
    .filter((F.col("created_at") > F.lit("2025-03-02").cast("date")) & F.col("code").isNotNull())
    .withColumn("code_lower", F.lower("code"))
    .withColumn("created_at_label", F.date_format("created_at", "MMM yyyy"))
)

filtered = (
    base
    .filter((F.col("created_at") >= F.to_timestamp(F.lit("2025-03-01 17:00:00"))) & F.col("code").isNotNull())
    .withColumn("code_upper", F.upper("code"))
    .withColumn("resolved_code", F.coalesce("code", F.lit("NONE")))
)

base_carts = (
    filtered
    .filter((F.col("created_at") >= F.lit("2025-03-01").cast("date")) & F.col("code").isin(["SPRING10", "WELCOME5", "BUNDLE"]))
    .withColumn("code_title", F.initcap(F.trim("code")))
    .withColumn("code_clean", F.coalesce("code", F.lit("UNKNOWN")))
)

kept_keys = (
    base_carts
    .select("id")
    .subtract(
        df_carts
        .filter(F.array_contains("coupon_codes", "BUNDLE"))
        .select("id")
    )
)

verified = (
    base_carts
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

filtered_carts = (
    verified
    .filter(F.col("code").isNotNull() & (F.col("created_at") >= "2025-03-01"))
    .select("*", F.lower("code").alias("code_lower_2"))
)

filtered_carts_2 = (
    filtered_carts
    .filter((F.col("created_at") >= F.to_timestamp(F.lit("2025-03-01 18:00:00"))) & F.col("code").isNotNull())
    .withColumn("code_group", F.when(F.col("code").isin(["BUNDLE", "SPRING10"]), "Core").otherwise("Other"))
)

result = (
    filtered_carts_2
    .withColumn("activity_day", F.date_trunc("DAY", "created_at"))
    .groupingSets([["code", "activity_day"], ["code"]], "code", "activity_day")
    .agg(
        F.size(F.collect_set("code")).alias("num_codes"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count(F.when(F.col("code") == "WELCOME5", 1)).alias("welcome5_count"),
        F.size(F.collect_set("code")).alias("num_codes_2"),
        F.sort_array(F.collect_set("code")).alias("code_list"),
        F.size(F.collect_set("code")).alias("num_codes_3"),
    )
    .filter(F.col("distinct_ids") > 1)
)
