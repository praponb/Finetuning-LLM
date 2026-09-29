from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("holder_id", "status").orderBy(F.col("days_end_date_to_start_date").desc(), F.col("id").desc())
w_all_by_days_end_date_to_star = Window.orderBy(F.col("days_end_date_to_start_date").desc(), "id")

scoped_policys = (
    df_policies
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "end_date",
        "status",
        F.add_months(F.col("start_date"), 12).alias("start_date_plus_12m"),
        F.datediff(F.col("start_date"), F.col("end_date")).alias("days_end_date_to_start_date"),
    )
)

with_details = (
    scoped_policys.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.status",
        "s.start_date_plus_12m",
        "s.days_end_date_to_start_date",
        F.col("p.joined_on").alias("holder_joined_on"),
        F.col("p.segment").alias("holder_segment"),
    )
)

deduped = (
    with_details
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

ranked = (
    deduped
    .select(
        "id",
        "holder_id",
        "start_date_plus_12m",
        "holder_joined_on",
        "holder_segment",
        F.round(F.percent_rank().over(w_all_by_days_end_date_to_star), 3).alias("days_end_date_to_start_date_percent_rank"),
        F.lag("start_date_plus_12m").over(w_all_by_days_end_date_to_star).alias("prev_start_date_plus_12m"),
    )
)

with_parent_holder = (
    ranked.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.start_date_plus_12m",
        "s.holder_joined_on",
        "s.holder_segment",
        "s.days_end_date_to_start_date_percent_rank",
        "s.prev_start_date_plus_12m",
        F.col("p.joined_on").alias("holder_joined_on_2"),
        F.col("p.region").alias("holder_region"),
        F.col("p.segment").alias("holder_segment_2"),
    )
)

detail_rows = (
    with_parent_holder.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.col("c.claimed_amount").between(1705.6, 43724.9)), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.holder_joined_on",
        "s.holder_segment",
        "s.holder_joined_on_2",
        "s.holder_region",
        "s.holder_segment_2",
        F.col("c.id").alias("claim_id"),
        F.col("c.adjuster_id").alias("claim_adjuster_id"),
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
    )
)

result = (
    detail_rows
    .groupBy("holder_joined_on", "claim_adjuster_id")
    .pivot("holder_segment", ["SME", "VIP"])
    .agg(F.count("claim_claimed_amount").alias("count_v"), F.max("claim_claimed_amount").alias("max_v"))
)
