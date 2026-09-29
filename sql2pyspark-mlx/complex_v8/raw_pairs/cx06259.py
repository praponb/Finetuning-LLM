from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("priority").orderBy(F.col("opened_at").desc(), F.col("id").desc())

cleaned_tickets = (
    df_tickets
    .filter(F.col("closed_at") < F.lit("2025-02-26 16:00:00").cast("timestamp"))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.concat(F.col("priority"), F.lit("#"), F.col("agent_id").cast("string")).alias("priority_ref"),
        F.concat(F.col("priority"), F.lit("#"), F.col("agent_id").cast("string")).alias("priority_ref_2"),
    )
)

unique_rows = (
    cleaned_tickets
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

totals = (
    unique_rows
    .filter(F.col("priority").isNotNull())
    .groupBy("priority", "csat")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("n_rows"),
        F.sum(F.when(F.col("priority") == "P4", F.col("csat"))).alias("csat_priority_p4"),
        F.array_sort(F.collect_set("channel")).alias("channel_list"),
        F.round(F.avg("csat"), 2).alias("avg_csat"),
    )
)

scored = totals.withColumn("priority_len", F.length(F.trim(F.col("priority"))))

result = (
    scored
    .select(
        "priority",
        "csat",
        "n_rows",
        "channel_list",
        "avg_csat",
        F.least(F.col("n_rows"), F.col("csat_priority_p4")).alias("least_n_rows_csat_priority_p4"),
    )
    .orderBy("n_rows")
)
