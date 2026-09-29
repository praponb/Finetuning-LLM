from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_channel_len_moving_6 = Window.orderBy(F.col("channel_len").desc(), "channel").rowsBetween(-6, Window.currentRow)
w_channel_group_channel = Window.partitionBy("channel_group", "channel")
w_all_by_channel_len_ties = Window.orderBy(F.col("channel_len").desc())

scoped_tickets = (
    df_tickets
    .filter((F.col("agent_id").isNotNull()) & (F.col("closed_at") >= F.lit("2025-01-17 21:00:00").cast("timestamp")))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "priority",
        "channel",
        F.to_date(F.col("closed_at")).alias("closed_at_date"),
    )
)

annotated = (
    scoped_tickets
    .filter(F.col("channel") == "web")
    .select(
        "id",
        "agent_id",
        "opened_at",
        "priority",
        "channel",
        F.to_date(F.col("opened_at")).alias("opened_at_date"),
        F.concat(F.col("channel"), F.lit("#"), F.col("id").cast("string")).alias("channel_ref"),
    )
)

stats = (
    annotated
    .filter(F.to_date(F.col("opened_at")) < F.lit("2025-02-06").cast("date"))
    .groupBy("channel")
    .agg(
        F.count(F.when(~F.col("channel").isin("email", "phone"), 1)).alias("n_channel_not_in_email_phone"),
        F.min("opened_at_date").alias("first_opened_at_date"),
    )
)

tagged = (
    stats
    .select(
        "channel",
        "n_channel_not_in_email_phone",
        "first_opened_at_date",
        F.length(F.trim(F.col("channel"))).alias("channel_len"),
        F.when(F.col("channel").isin("web", "chat"), "group_a").when(F.col("channel") == "email", "group_b").otherwise("other").alias("channel_group"),
        F.when(F.col("channel").isin("chat", "web"), "group_a").when(F.col("channel") == "phone", "group_b").otherwise("other").alias("channel_group_2"),
    )
)

channel_group_stats = (
    tagged
    .groupBy("channel_group")
    .agg(F.max("n_channel_not_in_email_phone").alias("max_n_channel_not_in_email_phone"))
)

vs_group = (
    tagged.alias("s")
    .join(channel_group_stats.alias("g"), F.col("g.channel_group") == F.col("s.channel_group"), "inner")
    .filter(F.col("s.n_channel_not_in_email_phone") == F.col("g.max_n_channel_not_in_email_phone"))
    .select(
        "s.*",
        "g.max_n_channel_not_in_email_phone",
        F.round(F.col("s.n_channel_not_in_email_phone") - F.col("g.max_n_channel_not_in_email_phone"), 2).alias("n_channel_not_in_email_phone_minus_max"),
    )
)

enriched = (
    vs_group
    .withColumn("channel_group_ref", F.concat(F.col("channel_group"), F.lit("#"), F.col("channel_len").cast("string")))
)

with_windows = (
    enriched
    .select(
        "*",
        F.sum("n_channel_not_in_email_phone").over(w_all_by_channel_len_moving_6).alias("n_channel_not_in_email_phone_sum_7"),
        F.size(F.collect_set("channel_group_2").over(w_channel_group_channel)).alias("distinct_channel_group_2_in_channel_group_channe"),
        F.rank().over(w_all_by_channel_len_ties).alias("channel_len_rank"),
    )
)

result = (
    with_windows
    .select(
        "channel",
        "n_channel_not_in_email_phone",
        "first_opened_at_date",
        "channel_len",
        "channel_group_2",
        "max_n_channel_not_in_email_phone",
        "n_channel_not_in_email_phone_minus_max",
        "channel_group_ref",
        "n_channel_not_in_email_phone_sum_7",
    )
    .orderBy(F.col("channel_len").desc(), "channel")
    .limit(5)
)
