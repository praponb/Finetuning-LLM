from pyspark.sql import functions as F

recent = (
    df_policies
    .filter((F.col("premium").between(1541.8, 4417.0)) & (F.col("product") != "TRAVEL"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.datediff(F.col("end_date"), F.col("start_date")).alias("days_start_date_to_end_date"),
    )
)

without_matches = (
    recent.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left_semi")
)

result = (
    without_matches
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "end_date",
        "status",
        "days_start_date_to_end_date",
        F.date_trunc("month", F.col("end_date")).alias("end_date_month"),
    )
)
