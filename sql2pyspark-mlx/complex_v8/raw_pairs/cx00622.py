from pyspark.sql import functions as F

prepared_policys = (
    df_policies
    .filter((F.year(F.col("start_date")) == 2025) & (F.col("end_date") < F.lit("2026-04-25").cast("date")))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
    )
)

derived = (
    prepared_policys
    .select(
        "*",
        F.date_format(F.col("end_date"), "yyyy-MM").alias("end_date_label"),
        F.datediff(F.col("end_date"), F.col("start_date")).alias("days_start_date_to_end_date"),
    )
)

policy_stats = (
    df_policies
    .groupBy("holder_id")
    .agg(F.min("start_date").alias("first_policy_start_date"))
)

summarized_rows = (
    derived.alias("s")
    .join(policy_stats.alias("p"), F.col("p.holder_id") == F.col("s.holder_id"), "left")
    .select("s.*", "p.first_policy_start_date")
)

result = (
    summarized_rows
    .groupBy("start_date")
    .pivot("status", ["LAPSED", "CANCELLED"])
    .agg(F.sum("days_start_date_to_end_date"))
)
