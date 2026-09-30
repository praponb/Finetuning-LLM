from pyspark.sql import functions as F

valid = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "plan",
        "start_date",
        "end_date",
        F.round((F.col("monthly_fee") * 0.85), 2).alias("adjusted_monthly_fee"),
    )
)

matched_keys = (
    valid
    .select("id", "plan")
    .subtract(
        df_subscriptions
        .filter(F.col("customer_id").isin([13, 14, 24, 37]))
        .select("id", "plan")
    )
)

reconciled = (
    valid
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.plan") == F.col("k.plan")), "inner")
    .select("s.*")
)

cleaned = (
    reconciled
    .filter(F.col("start_date") >= F.lit("2023-05-08").cast("date"))
    .withColumn("adjusted_monthly_fee_band", F.when(F.col("adjusted_monthly_fee") >= 75, "High").when(F.col("adjusted_monthly_fee") >= 32, "Medium").otherwise("Low"))
)

cleaned_subscriptions = (
    cleaned
    .filter((F.col("start_date") >= F.to_date(F.lit("2023-09-01"))) & (F.col("adjusted_monthly_fee") <= 61))
    .select(
        "id",
        "start_date",
        "adjusted_monthly_fee",
        "adjusted_monthly_fee_band",
        F.date_format("end_date", "yyyy-MM").alias("end_date_label"),
    )
)

filtered = (
    cleaned_subscriptions
    .filter(F.col("adjusted_monthly_fee") < 21)
    .select(
        "start_date",
        "adjusted_monthly_fee",
        "end_date_label",
        F.date_trunc("month", "start_date").alias("start_date_period"),
        F.datediff(F.current_date(), "start_date").alias("days_since_start_date"),
    )
)

result = (
    filtered
    .withColumn("adjusted_monthly_fee_tier", F.when(F.col("adjusted_monthly_fee") >= 61, "PREMIUM").otherwise("STANDARD"))
    .withColumn("year_start", F.date_trunc("YEAR", "start_date_period"))
    .withColumn("days_since_start_date_tier", F.when(F.col("days_since_start_date") >= 85, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["adjusted_monthly_fee_tier", "year_start", "days_since_start_date_tier"], ["adjusted_monthly_fee_tier", "year_start"], ["adjusted_monthly_fee_tier"]],
        "adjusted_monthly_fee_tier",
        "year_start",
        "days_since_start_date_tier",
    )
    .agg(
        F.max("start_date_period").alias("last_seen"),
        F.min("start_date_period").alias("first_start_date_period"),
        ((F.unix_timestamp(F.max("start_date_period")) - F.unix_timestamp(F.min("start_date_period"))) / 60).alias("duration_min"),
        F.round(F.avg("days_since_start_date"), 2).alias("avg_days_since_start_date"),
        F.round(F.avg("days_since_start_date"), 2).alias("avg_days_since_start_date_2"),
        ((F.unix_timestamp(F.max("start_date_period")) - F.unix_timestamp(F.min("start_date_period"))) / 60).alias("duration_min_2"),
    )
    .filter(F.col("avg_days_since_start_date_2") > 0)
    .orderBy(F.col("avg_days_since_start_date").asc_nulls_last())
)
