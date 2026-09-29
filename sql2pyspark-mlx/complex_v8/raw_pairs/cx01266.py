from pyspark.sql import functions as F

base_policys = (
    df_policies
    .filter(F.col("start_date") < F.lit("2025-03-21").cast("date"))
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "end_date",
        "status",
    )
)

scored = (
    base_policys
    .filter(F.col("start_date").between(F.lit("2024-11-20").cast("date"), F.lit("2025-03-23").cast("date")))
    .select(
        "*",
        F.date_add(F.col("start_date"), 30).alias("start_date_plus_30d"),
        F.dayofweek(F.col("start_date")).alias("start_date_dayofweek"),
    )
)

combined_details = (
    scored.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .filter(F.col("p.region") != "South")
    .select(
        "s.id",
        "s.holder_id",
        "s.start_date",
        "s.end_date",
        "s.status",
        "s.start_date_plus_30d",
        "s.start_date_dayofweek",
        F.col("p.region").alias("holder_region"),
        F.col("p.joined_on").alias("holder_joined_on"),
        F.col("p.risk_score").alias("holder_risk_score"),
    )
)

result = (
    combined_details
    .groupingSets(
        [["status", "holder_region", "holder_id"], ["status"], ["holder_region"], []],
        "status", "holder_region", "holder_id",
    )
    .agg(
        F.grouping("status").alias("is_total_status"),
        F.round(F.sum("holder_risk_score") / F.nullif(F.sum("start_date_dayofweek"), F.lit(0)), 3).alias("holder_risk_score_to_start_date_dayofweek_ratio"),
    )
    .select(
        F.coalesce(F.col("status"), F.lit("ALL")).alias("status"),
        F.coalesce(F.col("holder_region"), F.lit("ALL")).alias("holder_region"),
        "holder_id",
        "is_total_status",
        "holder_risk_score_to_start_date_dayofweek_ratio",
    )
)
