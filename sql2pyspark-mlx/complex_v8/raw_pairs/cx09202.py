from pyspark.sql import functions as F

filtered_policys = (
    df_policies
    .filter(F.col("end_date") < F.lit("2026-04-25").cast("date"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.add_months(F.col("end_date"), 3).alias("end_date_plus_3m"),
        F.coalesce(F.col("premium"), F.lit(0.0)).alias("premium_filled"),
        F.abs(F.col("premium") - 3465.4).alias("premium_distance"),
    )
)

rollup_base = (
    filtered_policys
    .filter(F.col("holder_id").isNotNull())
    .groupBy(F.date_format("end_date", "yyyy-MM").alias("end_date_ym"))
    .agg(
        F.sum("premium_filled").alias("total_premium_filled"),
        F.max("end_date_plus_3m").alias("last_end_date_plus_3m"),
        F.avg("premium").alias("avg_premium_check"),
    )
    .filter(F.col("avg_premium_check") > 1132.1)
    .drop("avg_premium_check")
)

tagged = (
    rollup_base
    .select(
        "*",
        F.round(F.col("total_premium_filled") * 0.25, 2).alias("total_premium_filled_adj"),
        F.lower(F.trim(F.col("end_date_ym"))).alias("end_date_ym_clean"),
    )
)

result = (
    tagged
    .select(
        "end_date_ym",
        "total_premium_filled",
        "last_end_date_plus_3m",
        "total_premium_filled_adj",
        "end_date_ym_clean",
        F.date_trunc("month", F.col("last_end_date_plus_3m")).alias("last_end_date_plus_3m_month"),
        F.when(F.col("total_premium_filled") >= 25530.0, "large").when(F.col("total_premium_filled") >= 13610.0, "medium").otherwise("small").alias("total_premium_filled_tier"),
    )
    .orderBy("last_end_date_plus_3m_month", "end_date_ym")
    .limit(5)
)
