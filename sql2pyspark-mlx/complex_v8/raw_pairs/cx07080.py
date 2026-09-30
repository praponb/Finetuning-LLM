from pyspark.sql import functions as F

scoped = (
    df_policies
    .filter((F.col("end_date") < F.lit("2026-07-11").cast("date")) | (F.col("start_date").isNotNull()))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.lower(F.regexp_replace(F.trim(F.col("status")), " ", "_")).alias("status_slug"),
        F.month(F.col("end_date")).alias("end_date_month"),
    )
)

screened = (
    scoped
    .crossJoin(scoped.agg(F.min("end_date_month").alias("overall_min_end_date_month")))
    .filter(F.col("end_date_month") > F.col("overall_min_end_date_month"))
    .drop("overall_min_end_date_month")
)

result = (
    screened
    .groupingSets(
        [["end_date_month", "status"], ["end_date_month"], ["status"], []],
        "end_date_month", "status",
    )
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.datediff(F.max("end_date"), F.min("end_date")).alias("end_date_span_days"),
        F.count("*").alias("n_rows"),
        F.min("end_date_month").alias("min_end_date_month"),
    )
    .select(
        "end_date_month",
        F.coalesce(F.col("status"), F.lit("ALL")).alias("status"),
        "grouping_level",
        "end_date_span_days",
        "n_rows",
        "min_end_date_month",
    )
    .orderBy(
        F.col("end_date_month").asc_nulls_last(),
        F.col("status").asc_nulls_last(),
    )
)
