from pyspark.sql import functions as F

exploded = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("tier_limits").alias("val"),
    )
)

filtered = (
    exploded
    .filter(F.col("val") >= 78)
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        "val",
        F.concat_ws("/", "currency", "plan").alias("currency_plan_key"),
    )
)

cleaned = (
    filtered
    .filter(F.col("currency") != "THB")
    .select(
        "id",
        "effective_from",
        "currency",
        "val",
        "currency_plan_key",
        F.date_add("effective_from", 67).alias("effective_from_plus_67d"),
        F.weekofyear("effective_from").alias("effective_from_weekofyear"),
    )
)

valid = (
    cleaned
    .filter(F.month("effective_from").isin([3, 10, 12]))
    .withColumn("val_num", F.col("val").cast("string"))
)

result = (
    valid
    .groupBy(F.date_trunc("YEAR", "effective_from").alias("year_start"), "currency")
    .agg(
        F.count_if(F.col("currency") == "THB").alias("thb_events"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy("year_start")
)
