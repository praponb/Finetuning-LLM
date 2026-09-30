from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_product = Window.partitionBy("product")
w_all_by_end_date_full = Window.orderBy(F.col("end_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_end_date = Window.orderBy(F.col("end_date").desc(), "id")

filtered_policys = (
    df_policies
    .filter((~F.col("product").isin("AUTO", "HOME")) & (F.col("holder_id") >= 3))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "end_date",
        "status",
        F.concat(F.col("status"), F.lit("#"), F.col("id").cast("string")).alias("status_ref"),
    )
)

policy_stats = (
    df_policies
    .groupBy("holder_id")
    .agg(F.min("end_date").alias("first_policy_end_date"))
)

with_counts = (
    filtered_policys.alias("s")
    .join(policy_stats.alias("p"), F.col("p.holder_id") == F.col("s.holder_id"), "left")
    .select("s.*", "p.first_policy_end_date")
)

with_details_holder = (
    with_counts.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select("s.*", F.col("p.risk_score").alias("holder_risk_score"))
)

with_activity = (
    with_details_holder.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.holder_id")) & (F.col("p.premium") > 2013.8) & (F.col("p.status") == "ACTIVE"), "left_anti")
    .filter(F.col("product").isin("HEALTH", "LIFE", "HOME"))
)

with_windows = (
    with_activity
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "end_date",
        "status",
        "status_ref",
        "first_policy_end_date",
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_product), F.lit(0)), 4).alias("premium_share_of_product"),
        F.first("end_date").over(w_all_by_end_date_full).alias("first_end_date"),
        F.lead("end_date", 3).over(w_all_by_end_date).alias("next3_end_date"),
        F.datediff(F.col("first_policy_end_date"), F.lag("first_policy_end_date").over(w_all_by_end_date)).alias("days_since_prev_first_policy_end_date"),
    )
)

combined_details = (
    with_windows.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.premium",
        "s.status",
        "s.premium_share_of_product",
        "s.first_end_date",
        "s.days_since_prev_first_policy_end_date",
        F.col("p.region").alias("holder_region"),
        F.col("p.joined_on").alias("holder_joined_on"),
    )
)

with_parent_holder = (
    combined_details.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.premium",
        "s.status",
        "s.premium_share_of_product",
        "s.first_end_date",
        "s.days_since_prev_first_policy_end_date",
        "s.holder_region",
        F.col("p.segment").alias("holder_segment"),
    )
)

result = (
    with_parent_holder
    .select(
        "id",
        "premium",
        "status",
        "premium_share_of_product",
        "days_since_prev_first_policy_end_date",
        "holder_region",
        "holder_segment",
        F.length(F.trim(F.col("holder_region"))).alias("holder_region_len"),
        F.concat_ws("-", F.col("status"), F.col("holder_segment")).alias("status_holder_segment_label"),
    )
    .orderBy("premium", "id")
    .limit(5)
)
