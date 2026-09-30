from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_total_policy_premium = Window.orderBy(F.col("total_policy_premium").desc(), "id")
w_last_policy_end_date_year_st = Window.partitionBy(F.year("last_policy_end_date"), "status_group")

cleaned_policys = (
    df_policies
    .filter(~F.col("product").isin("AUTO", "HOME"))
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "end_date",
        "status",
        F.when(F.col("status").isin("ACTIVE", "EXPIRED"), "group_a").when(F.col("status") == "LAPSED", "group_b").otherwise("other").alias("status_group"),
    )
)

with_parent_holder = (
    cleaned_policys.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.start_date",
        "s.end_date",
        "s.status_group",
        F.col("p.joined_on").alias("holder_joined_on"),
        F.col("p.risk_score").alias("holder_risk_score"),
    )
)

policy_stats = (
    df_policies
    .groupBy("holder_id")
    .agg(
        F.sum("premium").alias("total_policy_premium"),
        F.max("end_date").alias("last_policy_end_date"),
    )
)

summarized_rows = (
    with_parent_holder.alias("s")
    .join(policy_stats.alias("p"), F.col("p.holder_id") == F.col("s.holder_id"), "left")
    .select("s.*", "p.total_policy_premium", "p.last_policy_end_date")
)

enriched_holder = (
    summarized_rows.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.start_date",
        "s.end_date",
        "s.status_group",
        "s.holder_joined_on",
        "s.holder_risk_score",
        "s.total_policy_premium",
        "s.last_policy_end_date",
        "p.holder_name",
        F.col("p.joined_on").alias("holder_joined_on_2"),
    )
)

with_windows = (
    enriched_holder
    .select(
        "id",
        "holder_id",
        "end_date",
        "holder_joined_on",
        "holder_risk_score",
        "last_policy_end_date",
        "holder_name",
        "holder_joined_on_2",
        F.lead("holder_risk_score", 2).over(w_all_by_total_policy_premium).alias("next2_holder_risk_score"),
        F.size(F.collect_set("holder_id").over(w_last_policy_end_date_year_st)).alias("distinct_holder_id_in_last_policy_end_date_year_"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "holder_id",
        "end_date",
        "holder_joined_on",
        "holder_risk_score",
        "last_policy_end_date",
        "holder_name",
        "holder_joined_on_2",
        "next2_holder_risk_score",
        "distinct_holder_id_in_last_policy_end_date_year_",
        F.round(F.col("next2_holder_risk_score") / F.nullif(F.col("holder_risk_score"), F.lit(0)), 3).alias("next2_holder_risk_score_per_holder_risk_score"),
    )
)
