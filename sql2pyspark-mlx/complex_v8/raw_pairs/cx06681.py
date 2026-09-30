from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_closed_at_ties = Window.orderBy(F.col("closed_at").desc_nulls_first())

prepared = (
    df_tickets
    .filter(F.col("csat") < 4)
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "tags",
        F.date_trunc("month", F.col("opened_at")).alias("opened_at_month"),
        F.concat(F.col("priority"), F.lit("#"), F.col("agent_id").cast("string")).alias("priority_ref"),
        F.greatest(F.col("csat"), F.col("requester_id")).alias("greatest_csat_requester_id"),
    )
)

enriched = (
    prepared
    .select(
        "id",
        "closed_at",
        "tags",
        "priority_ref",
        F.substring(F.trim(F.col("priority_ref")), 1, 4).alias("priority_ref_prefix"),
        (F.floor(F.col("requester_id") / 5) * 5).alias("requester_id_bucket"),
    )
)

with_windows = enriched.withColumn("closed_at_rank", F.rank().over(w_all_by_closed_at_ties))

result = (
    with_windows
    .select(
        "id",
        "closed_at",
        "tags",
        "priority_ref_prefix",
        "requester_id_bucket",
        "closed_at_rank",
        F.when(F.col("closed_at_rank") >= 6, "large").when(F.col("closed_at_rank") >= 5, "medium").otherwise("small").alias("closed_at_rank_tier"),
    )
)
