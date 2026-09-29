from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("channel").orderBy(F.col("closed_at_month").desc(), F.col("id").desc())

valid = (
    df_tickets
    .filter((F.col("requester_id") <= 17) & (~F.array_contains(F.col("tags"), "login")))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.when(F.col("channel").isin("email", "web"), "group_a").when(F.col("channel") == "phone", "group_b").otherwise("other").alias("channel_group"),
    )
)

derived = (
    valid
    .filter(F.size(F.col("tags")) >= 2)
    .select(
        "*",
        F.month(F.col("closed_at")).alias("closed_at_month"),
        F.abs(F.col("csat") - 2).alias("csat_distance"),
    )
)

unique_rows = (
    derived
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    unique_rows
    .select(
        "id",
        "agent_id",
        "closed_at",
        "tags",
        "closed_at_month",
        "csat_distance",
        F.round(F.col("csat") / F.nullif(F.col("requester_id"), F.lit(0)), 3).alias("csat_per_requester_id"),
    )
    .orderBy("id")
    .limit(10)
)
