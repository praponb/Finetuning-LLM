from pyspark.sql import functions as F

valid_policys = (
    df_policies
    .filter((F.col("status").isNotNull()) & (F.col("holder_id").isNotNull()))
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.date_trunc("quarter", F.col("end_date")).alias("end_date_quarter"),
    )
)

combined_details_holder = (
    valid_policys.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.premium",
        "s.start_date",
        "s.status",
        "s.end_date_quarter",
        F.col("p.segment").alias("holder_segment"),
    )
)

with_details = (
    combined_details_holder.alias("s")
    .join(df_policyholders.alias("p"), (F.col("p.id") == F.col("s.holder_id")) & (F.col("p.region").isNotNull()), "inner")
    .filter(F.col("p.risk_score").between(0.1, 0.9))
    .select(
        "s.*",
        F.col("p.risk_score").alias("holder_risk_score"),
        F.col("p.segment").alias("holder_segment_2"),
        F.col("p.region").alias("holder_region"),
    )
)

labeled = (
    with_details
    .filter(F.col("start_date") < F.lit("2025-08-03").cast("date"))
    .select(
        "*",
        F.date_trunc("day", F.col("end_date_quarter")).alias("end_date_quarter_day"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("start_date")).alias("days_since_start_date"),
    )
)

result = (
    labeled
    .filter(F.col("end_date_quarter") < F.lit("2025-02-05 09:00:00").cast("timestamp"))
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "end_date_quarter",
        "holder_risk_score",
        "holder_segment_2",
        "holder_region",
        "end_date_quarter_day",
        "days_since_start_date",
        F.when(F.col("holder_segment").isin("VIP", "RETAIL"), "group_a").when(F.col("holder_segment") == "SME", "group_b").otherwise("other").alias("holder_segment_group"),
        F.when(F.col("premium") >= 2945.9, "A").when(F.col("premium") >= 2096.6, "B").otherwise("C").alias("premium_tier"),
    )
    .orderBy("holder_segment_2", F.col("id").desc_nulls_first())
    .limit(5)
)
