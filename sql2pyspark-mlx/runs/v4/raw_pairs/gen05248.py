from pyspark.sql import functions as F

exploded = df_carts.select("id", "created_at", F.explode("coupon_codes").alias("value"))

kept_keys = (
    exploded
    .select("id")
    .exceptAll(
        df_carts
        .filter(F.col("customer_id") == 19)
        .select("id")
    )
)

kept_rows = (
    exploded
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

cleaned = (
    kept_rows
    .filter((F.col("created_at") > F.to_timestamp(F.lit("2025-03-02 09:00:00"))) & F.col("value").isNotNull())
    .select("*", F.trim("value").alias("clean_value"))
)

filtered = (
    cleaned
    .filter(F.hour("created_at").between(9, 15))
    .withColumn("value_title", F.initcap(F.trim("value")))
    .withColumn("created_at_period", F.date_trunc("WEEK", "created_at"))
    .withColumn("value_filled", F.coalesce("value", F.lit("UNKNOWN")))
)

result = (
    filtered
    .withColumn("activity_month", F.date_trunc("MONTH", "created_at_period"))
    .groupingSets(
        [["activity_month", "value"], ["activity_month"]],
        "activity_month",
        "value",
    )
    .agg(
        F.min("created_at").alias("started_at"),
        F.size(F.collect_set("value")).alias("num_values"),
        ((F.unix_timestamp(F.max("created_at_period")) - F.unix_timestamp(F.min("created_at_period"))) / 60).alias("span_minutes"),
        F.max("created_at_period").alias("last_created_at_period"),
    )
    .orderBy("activity_month")
)
