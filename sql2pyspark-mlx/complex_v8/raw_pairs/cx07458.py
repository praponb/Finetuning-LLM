from pyspark.sql import functions as F

valid_tickets = (
    df_tickets
    .filter((F.col("agent_id").isNotNull()) & (F.hour(F.col("closed_at")).between(9, 12)))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "tags",
        F.date_format(F.col("opened_at"), "MMM yyyy").alias("opened_at_label"),
        F.when(F.col("channel").isin("email", "web"), "group_a").when(F.col("channel") == "phone", "group_b").otherwise("other").alias("channel_group"),
    )
)

summary = (
    valid_tickets
    .groupBy(F.year("closed_at").alias("closed_at_year"))
    .agg(
        F.min("opened_at").alias("first_opened_at"),
        F.min("closed_at").alias("first_closed_at"),
    )
)

derived = (
    summary
    .select(
        "*",
        F.round((F.unix_timestamp(F.col("first_closed_at")) - F.unix_timestamp(F.col("first_opened_at"))) / 3600, 1).alias("hours_first_opened_at_to_first_closed_at"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("first_closed_at")).alias("days_since_first_closed_at"),
    )
)

scored = (
    derived
    .filter(F.col("first_closed_at") < F.lit("2025-02-10 04:00:00").cast("timestamp"))
    .select(
        "*",
        F.coalesce(F.col("closed_at_year"), F.lit(0)).alias("closed_at_year_filled"),
        F.round((F.unix_timestamp(F.col("first_closed_at")) - F.unix_timestamp(F.col("first_opened_at"))) / 3600, 1).alias("hours_first_opened_at_to_first_closed_at_2"),
    )
)

result = (
    scored
    .cube("closed_at_year", "closed_at_year_filled")
    .agg(
        F.max("first_closed_at").alias("last_first_closed_at"),
        F.count("*").alias("n_rows"),
    )
    .orderBy(
        F.col("closed_at_year").asc_nulls_last(),
        F.col("closed_at_year_filled").asc_nulls_last(),
    )
)
