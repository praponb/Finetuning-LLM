from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("priority_group", "agent_id").orderBy(F.col("opened_at").desc(), F.col("id").desc())

filtered = (
    df_tickets
    .filter(F.to_date(F.col("closed_at")) < F.lit("2025-03-22").cast("date"))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "csat",
        "tags",
        F.round((F.unix_timestamp(F.col("opened_at")) - F.unix_timestamp(F.col("closed_at"))) / 60, 1).alias("minutes_closed_at_to_opened_at"),
        F.when(F.col("priority").isin("P1", "P3"), "group_a").when(F.col("priority") == "P2", "group_b").otherwise("other").alias("priority_group"),
        (F.floor(F.col("requester_id") / 5) * 5).alias("requester_id_bucket"),
    )
)

latest = (
    filtered
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    latest
    .cube("priority_group", "agent_id", "csat")
    .agg(
        F.sum("requester_id_bucket").alias("total_requester_id_bucket"),
        F.countDistinct("priority_group").alias("distinct_priority_groups"),
    )
)
