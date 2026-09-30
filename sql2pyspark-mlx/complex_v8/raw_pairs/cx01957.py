from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_joined_on_month_end_month_po = Window.partitionBy(F.month("joined_on_month_end"), F.year("policy_end_date")).orderBy(F.col("policy_end_date").desc())
w_joined_on_month_end_year_pol = Window.partitionBy(F.year("joined_on_month_end"), "policy_status")
w_joined_on_month_end_month_po_2 = Window.partitionBy(F.month("joined_on_month_end"), F.year("policy_end_date")).orderBy(F.col("policy_end_date").desc(), "id", "policy_id").rowsBetween(-1, Window.currentRow)

prepared_holders = (
    df_policyholders
    .filter(F.col("region").isNotNull())
    .select(
        "id",
        "region",
        "risk_score",
        "segment",
        F.last_day(F.col("joined_on")).alias("joined_on_month_end"),
    )
)

with_activity = (
    prepared_holders.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.id")) & (F.col("p.premium") < 4417.0), "left_anti")
)

line_level = (
    with_activity.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.id")) & (F.col("p.status").isin("LAPSED", "ACTIVE", "CANCELLED")), "left")
    .select(
        "s.id",
        "s.risk_score",
        "s.joined_on_month_end",
        F.col("p.id").alias("policy_id"),
        F.col("p.status").alias("policy_status"),
        F.col("p.end_date").alias("policy_end_date"),
    )
)

with_trends = (
    line_level
    .select(
        "*",
        F.round(F.percent_rank().over(w_joined_on_month_end_month_po), 3).alias("policy_end_date_percent_rank"),
        F.round(F.col("risk_score") - F.avg("risk_score").over(w_joined_on_month_end_year_pol), 2).alias("risk_score_vs_joined_on_month_end_year_policy_st"),
        F.round(F.avg("risk_score").over(w_joined_on_month_end_month_po_2), 2).alias("risk_score_avg_2"),
        F.count(F.lit(1)).over(w_joined_on_month_end_year_pol).alias("rows_in_joined_on_month_end_year_policy_status"),
    )
)

labeled = (
    with_trends
    .filter(F.col("risk_score").between(0.1, 0.7))
    .select(
        "*",
        F.substring(F.trim(F.col("policy_status")), 1, 2).alias("policy_status_prefix"),
        F.lower(F.trim(F.col("policy_status"))).alias("policy_status_clean"),
        F.concat(F.col("policy_status"), F.lit("#"), F.col("id").cast("string")).alias("policy_status_ref"),
    )
)

result = (
    labeled
    .select(
        "id",
        "policy_id",
        F.col("risk_score_avg_2").alias("risk_score_avg_2_v"),
        F.col("risk_score_vs_joined_on_month_end_year_policy_st").alias("risk_score_vs_joined_on_month_end_year_policy_st_2"),
        F.col("risk_score").alias("m_risk_score"),
        F.col("policy_end_date_percent_rank").alias("m_policy_end_date_percent_rank"),
    )
    .unpivot(
        ids=["id", "policy_id"],
        values=["risk_score_avg_2_v", "risk_score_vs_joined_on_month_end_year_policy_st_2", "m_risk_score", "m_policy_end_date_percent_rank"],
        variableColumnName="measure",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
