from pyspark.sql import functions as F

valid = (
    df_policies
    .filter((F.col("end_date").isNotNull()) & (F.col("start_date") < F.lit("2025-02-14").cast("date")))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.datediff(F.col("start_date"), F.col("end_date")).alias("days_end_date_to_start_date"),
        F.datediff(F.col("start_date"), F.col("end_date")).alias("days_end_date_to_start_date_2"),
    )
)

with_parent_holder = (
    valid.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .filter(F.col("p.segment").isin("RETAIL", "VIP"))
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.start_date",
        "s.end_date",
        "s.status",
        "s.days_end_date_to_start_date",
        "s.days_end_date_to_start_date_2",
        F.col("p.region").alias("holder_region"),
        F.col("p.risk_score").alias("holder_risk_score"),
        F.col("p.segment").alias("holder_segment"),
    )
)

result = (
    with_parent_holder
    .rollup("status", "holder_segment")
    .agg(F.count("*").alias("n_rows"))
    .select(
        F.coalesce(F.col("status"), F.lit("ALL")).alias("status"),
        F.coalesce(F.col("holder_segment"), F.lit("ALL")).alias("holder_segment"),
        "n_rows",
    )
)
