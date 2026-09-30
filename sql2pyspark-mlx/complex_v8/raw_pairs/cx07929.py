from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_id_by_end_date = Window.partitionBy("holder_id").orderBy(F.col("end_date").desc(), "id")
w_all_by_end_date_running = Window.orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_holder_id_by_end_date_full = Window.partitionBy("holder_id").orderBy(F.col("end_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_holder_id = Window.partitionBy("holder_id")

recent = (
    df_policies
    .filter((F.col("premium").between(1132.1, 3719.7)) & (F.col("product") == "LIFE"))
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "end_date",
    )
)

sequenced = (
    recent
    .select(
        "*",
        F.lag("start_date").over(w_holder_id_by_end_date).alias("prev_start_date"),
        F.sum("premium").over(w_all_by_end_date_running).alias("running_premium"),
        F.first("end_date").over(w_holder_id_by_end_date_full).alias("first_end_date"),
        F.round(F.avg("premium").over(w_holder_id), 2).alias("holder_id_avg_premium"),
        F.last("premium", ignorenulls=True).over(w_all_by_end_date_running).alias("last_known_premium"),
    )
)

without_matches = (
    sequenced.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.holder_id")) & (F.col("p.product") == "TRAVEL") & (F.col("p.status") == "CANCELLED"), "left_anti")
    .filter(F.col("start_date") >= F.lit("2024-07-19").cast("date"))
)

tagged = (
    without_matches
    .filter(F.col("running_premium") > 20120.0)
    .select(
        "*",
        (F.floor(F.col("last_known_premium") / 1000) * 1000).alias("last_known_premium_bucket"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("prev_start_date")).alias("days_since_prev_start_date"),
    )
)

tagged_2 = (
    tagged
    .filter(F.col("end_date").between(F.lit("2025-11-20").cast("date"), F.lit("2026-09-19").cast("date")))
    .select(
        "*",
        F.round(F.col("premium") / F.nullif(F.col("holder_id_avg_premium"), F.lit(0)), 3).alias("premium_per_holder_id_avg_premium"),
        F.round(F.col("running_premium") * 0.25, 2).alias("running_premium_adj"),
    )
)

detail_rows = (
    tagged_2.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.end_date",
        "s.prev_start_date",
        "s.running_premium",
        "s.first_end_date",
        "s.holder_id_avg_premium",
        "s.last_known_premium",
        "s.days_since_prev_start_date",
        F.col("c.id").alias("claim_id"),
        F.col("c.adjuster_id").alias("claim_adjuster_id"),
        "c.claim_type",
        F.col("c.status").alias("claim_status"),
    )
)

result = (
    detail_rows
    .cube("claim_status", "holder_id")
    .agg(
        F.array_sort(F.collect_set("claim_status")).alias("claim_status_list"),
        F.sum("last_known_premium").alias("total_last_known_premium"),
        F.round(F.sum("last_known_premium") / F.nullif(F.sum("holder_id_avg_premium"), F.lit(0)), 3).alias("last_known_premium_to_holder_id_avg_premium_rati"),
    )
    .select(
        F.coalesce(F.col("claim_status"), F.lit("ALL")).alias("claim_status"),
        "holder_id",
        "claim_status_list",
        "total_last_known_premium",
        "last_known_premium_to_holder_id_avg_premium_rati",
    )
    .orderBy(
        F.col("claim_status").asc_nulls_last(),
        F.col("holder_id").asc_nulls_last(),
    )
)
