from pyspark.sql import functions as F

filtered_tickets = (
    df_tickets
    .filter((F.col("requester_id").between(7, 24)) & (F.col("priority") == "P4"))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "channel",
        "csat",
        F.date_format(F.col("opened_at"), "yyyy-MM-dd").alias("opened_at_label"),
        F.concat(F.col("priority"), F.lit("#"), F.col("id").cast("string")).alias("priority_ref"),
        F.initcap(F.trim(F.col("priority"))).alias("priority_clean"),
    )
)

per_group = (
    filtered_tickets
    .groupBy(
        F.date_format("closed_at", "yyyy-MM").alias("closed_at_ym"),
        F.date_format("opened_at", "yyyy-MM").alias("opened_at_ym"),
        "csat",
    )
    .agg(
        F.sum(F.when(F.col("channel").isin("chat", "email", "phone"), 1).otherwise(0)).alias("n_channel_in_chat_email_phone"),
        F.countDistinct("channel").alias("distinct_channels"),
        F.array_sort(F.collect_set("channel")).alias("channel_list"),
        F.max("csat").alias("max_csat_check"),
    )
    .filter(F.col("max_csat_check") > 1)
    .drop("max_csat_check")
)

tagged = (
    per_group
    .filter(F.col("n_channel_in_chat_email_phone").between(2, 9))
    .select(
        "closed_at_ym",
        "opened_at_ym",
        "csat",
        "distinct_channels",
        "channel_list",
        F.upper(F.trim(F.col("closed_at_ym"))).alias("closed_at_ym_clean"),
        F.when(F.col("csat") >= 4, "large").when(F.col("csat") >= 2, "medium").otherwise("small").alias("csat_tier"),
    )
)

result = (
    tagged
    .groupBy("csat")
    .pivot("csat_tier", ["small", "large", "medium"])
    .agg(F.sum("distinct_channels").alias("amount"), F.max("distinct_channels").alias("max_v"))
)
