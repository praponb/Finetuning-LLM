from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_agent_hired_on = Window.orderBy(F.col("agent_hired_on").desc(), "id")
w_all_by_agent_hired_on_full = Window.orderBy(F.col("agent_hired_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_agent_hired_on_runnin = Window.orderBy(F.col("agent_hired_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_agent_tier_agent_id = Window.partitionBy("agent_tier", "agent_id")

filtered_tickets = (
    df_tickets
    .filter(F.size(F.col("tags")) >= 1)
    .select(
        "id",
        "requester_id",
        "agent_id",
        "channel",
        "tags",
        F.coalesce(F.col("csat"), F.lit(0)).alias("csat_filled"),
        (F.col("requester_id") + F.col("csat")).alias("requester_id_plus_csat"),
    )
)

combined_details = (
    filtered_tickets.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.channel",
        "s.tags",
        F.col("su.hired_on").alias("agent_hired_on"),
        F.col("su.tier").alias("agent_tier"),
    )
)

with_trends = (
    combined_details
    .select(
        "id",
        "requester_id",
        "agent_id",
        "agent_tier",
        F.lead("agent_hired_on", 3).over(w_all_by_agent_hired_on).alias("next3_agent_hired_on"),
        F.nth_value("requester_id", 2).over(w_all_by_agent_hired_on_full).alias("second_requester_id"),
        F.last("agent_hired_on", ignorenulls=True).over(w_all_by_agent_hired_on_runnin).alias("last_known_agent_hired_on"),
        F.round(F.col("requester_id") / F.nullif(F.sum("requester_id").over(w_agent_tier_agent_id), F.lit(0)), 4).alias("requester_id_share_of_agent_tier_agent_id"),
    )
)

rollup_base = (
    with_trends
    .filter(F.col("last_known_agent_hired_on").isNotNull())
    .groupBy(
        "agent_id",
        F.year("next3_agent_hired_on").alias("next3_agent_hired_on_year"),
    )
    .agg(
        F.countDistinct("agent_tier").alias("distinct_agent_tiers"),
        F.round(F.sum("requester_id_share_of_agent_tier_agent_id") / F.nullif(F.sum("second_requester_id"), F.lit(0)), 3).alias("requester_id_share_of_agent_tier_agent_id_to_sec"),
        F.sum("requester_id_share_of_agent_tier_agent_id").alias("total_requester_id_share_of_agent_tier_agent_id"),
        F.sum(F.when(F.col("agent_tier") != "T1", 1).otherwise(0)).alias("n_agent_tier_t1"),
        F.count("*").alias("n_rows"),
    )
    .filter(F.col("n_rows") >= 2)
)

labeled = (
    rollup_base
    .select(
        "*",
        (F.floor(F.col("n_rows") / 2) * 2).alias("n_rows_bucket"),
        (F.floor(F.col("n_rows") / 2) * 2).alias("n_rows_bucket_2"),
    )
)

qualified = (
    labeled.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.agent_id")) & (F.col("t.requester_id") >= 7), "left_anti")
)

annotated = (
    qualified
    .withColumn("distinct_agent_tiers_tier", F.when(F.col("distinct_agent_tiers") >= 7, "HIGH").when(F.col("distinct_agent_tiers") >= 5, "MEDIUM").otherwise("LOW"))
)

result = (
    annotated
    .groupBy("n_agent_tier_t1", "next3_agent_hired_on_year")
    .pivot("distinct_agent_tiers_tier", ["HIGH", "LOW"])
    .agg(F.sum("distinct_agent_tiers"))
    .orderBy("n_agent_tier_t1", "next3_agent_hired_on_year")
)
