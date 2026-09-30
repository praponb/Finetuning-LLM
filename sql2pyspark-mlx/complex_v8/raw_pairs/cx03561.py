from pyspark.sql import functions as F

valid = (
    df_policies
    .filter((F.col("start_date") < F.lit("2025-03-21").cast("date")) & (F.col("status") != "LAPSED"))
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "end_date",
        "status",
    )
)

with_details_holder = (
    valid.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .filter(F.length(F.trim(F.col("p.holder_name"))) >= 8)
    .select(
        "s.id",
        "s.holder_id",
        "s.status",
        "p.holder_name",
    )
)

with_activity = (
    with_details_holder.alias("s")
    .join(df_policyholders.alias("p"), (F.col("p.id") == F.col("s.holder_id")) & (F.upper(F.trim(F.col("p.holder_name"))).like("G%")) & (F.col("p.risk_score") > 0.1), "left_semi")
)

result = (
    with_activity
    .groupingSets(
        [["holder_id", "status"], ["holder_id"]],
        "holder_id", "status",
    )
    .agg(
        F.grouping("holder_id").alias("is_total_holder_id"),
        F.count("*").alias("n_rows"),
    )
    .select(
        "holder_id",
        F.coalesce(F.col("status"), F.lit("ALL")).alias("status"),
        "is_total_holder_id",
        "n_rows",
    )
    .orderBy(F.col("holder_id").asc_nulls_last(), F.col("status").asc_nulls_last())
)
