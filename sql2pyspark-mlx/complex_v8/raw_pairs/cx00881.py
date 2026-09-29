from pyspark.sql import functions as F

valid = (
    df_policies
    .filter(F.year(F.col("end_date")) == 2026)
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "status",
        F.abs(F.col("premium") - 2844.1).alias("premium_distance"),
    )
)

with_parent_holder = (
    valid.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.*",
        F.col("p.joined_on").alias("holder_joined_on"),
        F.col("p.risk_score").alias("holder_risk_score"),
    )
)

derived = (
    with_parent_holder
    .select(
        "*",
        F.datediff(F.col("start_date"), F.col("holder_joined_on")).alias("days_holder_joined_on_to_start_date"),
        F.upper(F.trim(F.col("status"))).alias("status_clean"),
    )
)

result = (
    derived
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "status",
        "premium_distance",
        "holder_joined_on",
        "holder_risk_score",
        "status_clean",
        F.initcap(F.trim(F.col("status"))).alias("status_clean_2"),
        F.upper(F.trim(F.col("status"))).alias("status_clean_3"),
    )
    .orderBy(F.col("status_clean_3").asc_nulls_last(), "id")
    .limit(5)
)
