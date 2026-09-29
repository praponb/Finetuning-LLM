from pyspark.sql import functions as F

enriched = (
    df_subscriptions
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.city"), F.col("p.segment"))
)

matched_keys = (
    enriched
    .select("id")
    .intersectAll(
        df_subscriptions
        .filter(F.col("end_date").between("2024-04-15", "2025-05-01"))
        .select("id")
    )
)

verified = (
    enriched
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

filtered = (
    verified
    .filter(F.col("end_date") > F.to_date(F.lit("2023-07-23")))
    .select(
        "customer_id",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        "city",
        "segment",
        F.round((F.datediff("end_date", "start_date") / 30), 1).alias("months_start_date_to_end_date"),
        F.coalesce("city", F.lit("N/A")).alias("city_clean"),
    )
)

joined = (
    filtered
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id") == F.col("p.id"), "inner")
    .select(
        "src.customer_id",
        "src.start_date",
        "src.segment",
        "src.months_start_date_to_end_date",
        F.col("p.city").alias("customer_city"),
        F.col("p.segment").alias("customer_segment"),
    )
)

cleaned = (
    joined
    .filter(F.col("start_date") > F.lit("2023-08-08").cast("date"))
    .select(
        "*",
        F.when(F.col("months_start_date_to_end_date") >= 73, "High").when(F.col("months_start_date_to_end_date") >= 29, "Medium").otherwise("Low").alias("months_start_date_to_end_date_band"),
        F.length("segment").alias("segment_length"),
    )
)

result = (
    cleaned
    .select(
        "segment",
        "months_start_date_to_end_date",
        "customer_city",
        "customer_segment",
        "months_start_date_to_end_date_band",
        "segment_length",
        F.when(F.col("customer_city").rlike("^(Berlin|Chiang Mai)$"), "OTHER_ZONE").when(F.col("customer_city").rlike("^(Bangkok|New York)$"), "CORE").otherwise("GROUP_B").alias("customer_city_zone"),
    )
    .orderBy(F.col("segment_length").desc())
)
