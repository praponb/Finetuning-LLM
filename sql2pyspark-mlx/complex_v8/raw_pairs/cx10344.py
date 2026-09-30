from pyspark.sql import functions as F

recent_policys = (
    df_policies
    .filter((F.col("start_date") < F.lit("2025-05-30").cast("date")) & (F.col("end_date").isNotNull()))
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.datediff(F.col("end_date"), F.col("start_date")).alias("days_start_date_to_end_date"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("end_date")).alias("days_since_end_date"),
        F.concat(F.col("status"), F.lit("#"), F.col("holder_id").cast("string")).alias("status_ref"),
    )
)

holder_id_stats = (
    recent_policys
    .groupBy("holder_id")
    .agg(F.percentile_approx("days_start_date_to_end_date", 0.5).alias("median_days_start_date_to_end_date"))
)

compared = (
    recent_policys.alias("s")
    .join(holder_id_stats.alias("g"), F.col("g.holder_id") == F.col("s.holder_id"), "inner")
    .filter(F.col("s.days_start_date_to_end_date") >= F.col("g.median_days_start_date_to_end_date"))
    .select(
        "s.*",
        "g.median_days_start_date_to_end_date",
        F.round(F.col("s.days_start_date_to_end_date") - F.col("g.median_days_start_date_to_end_date"), 2).alias("days_start_date_to_end_date_minus_median"),
    )
)

result = (
    compared
    .groupingSets(
        [["status", "holder_id"], ["status"], ["holder_id"]],
        "status", "holder_id",
    )
    .agg(
        F.sum(F.when(~F.col("status").isin("EXPIRED", "CANCELLED"), F.col("days_start_date_to_end_date"))).alias("days_start_date_to_end_date_status_not_in_expire"),
    )
    .select(
        F.coalesce(F.col("status"), F.lit("ALL")).alias("status"),
        "holder_id",
        "days_start_date_to_end_date_status_not_in_expire",
    )
)
