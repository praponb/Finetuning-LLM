from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("tier").orderBy(F.col("hired_on").desc(), F.col("id").desc())

valid_agents = (
    df_support_agents
    .select(
        "id",
        "agent_name",
        "tier",
        "hired_on",
    )
)

unique_rows = (
    valid_agents
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

tagged = (
    unique_rows
    .select(
        "*",
        F.date_format(F.col("hired_on"), "MMM yyyy").alias("hired_on_label"),
        F.trunc(F.col("hired_on"), "MM").alias("hired_on_month_start"),
    )
)

annotated = (
    tagged
    .filter(F.col("hired_on_month_start") < F.lit("2025-08-01").cast("date"))
    .select("*", F.substring(F.trim(F.col("tier")), 1, 2).alias("tier_prefix"))
)

filtered = (
    annotated.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.id")) & (F.col("t.requester_id") >= 9), "left_anti")
)

result = (
    filtered
    .select(
        "id",
        "hired_on",
        "hired_on_label",
        "hired_on_month_start",
        F.lower(F.regexp_replace(F.trim(F.col("tier")), " ", "_")).alias("tier_slug"),
    )
    .orderBy("tier_slug")
)
