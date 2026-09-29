from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_or_unknown_by_start_d = Window.partitionBy("status_or_unknown").orderBy(F.col("start_date").desc(), "id")
w_status_or_unknown_by_start_d_2 = Window.partitionBy("status_or_unknown").orderBy(F.col("start_date").desc())

base_policys = (
    df_policies
    .filter((F.col("status") == "LAPSED") | (F.col("end_date").between(F.lit("2025-04-17").cast("date"), F.lit("2027-03-20").cast("date"))))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.substring(F.trim(F.col("product")), 1, 3).alias("product_prefix"),
        F.coalesce(F.col("status"), F.lit("UNKNOWN")).alias("status_or_unknown"),
    )
)

enriched = (
    base_policys.alias("s")
    .join(df_policyholders.alias("p"), (F.col("p.id") == F.col("s.holder_id")) & (F.col("p.segment") == "RETAIL"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.start_date",
        "s.end_date",
        "s.status",
        "s.product_prefix",
        "s.status_or_unknown",
        F.col("p.risk_score").alias("holder_risk_score"),
    )
)

windowed = (
    enriched
    .select(
        "*",
        F.lead("holder_risk_score").over(w_status_or_unknown_by_start_d).alias("next_holder_risk_score"),
        F.round(F.percent_rank().over(w_status_or_unknown_by_start_d_2), 3).alias("start_date_percent_rank"),
    )
)

screened = (
    windowed.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.holder_id")) & (F.col("p.end_date").isNotNull()) & (F.col("p.start_date") >= F.lit("2024-10-22").cast("date")), "left_anti")
)

result = (
    screened
    .rollup("holder_id", "status_or_unknown", "status")
    .agg(
        F.grouping("holder_id").alias("is_total_holder_id"),
        F.count("*").alias("cnt"),
        F.round(F.sum("holder_risk_score"), 2).alias("total_holder_risk_score"),
        F.max("start_date").alias("last_start_date"),
    )
    .select(
        "holder_id",
        F.coalesce(F.col("status_or_unknown"), F.lit("ALL")).alias("status_or_unknown"),
        F.coalesce(F.col("status"), F.lit("ALL")).alias("status"),
        "is_total_holder_id",
        "cnt",
        "total_holder_risk_score",
        "last_start_date",
    )
    .orderBy(
        F.col("holder_id").asc_nulls_last(),
        F.col("status_or_unknown").asc_nulls_last(),
        F.col("status").asc_nulls_last(),
    )
)
