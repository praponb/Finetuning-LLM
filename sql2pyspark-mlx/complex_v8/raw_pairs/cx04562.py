from pyspark.sql import functions as F

valid = (
    df_policies
    .filter((F.col("product") != "HEALTH") & (F.col("holder_id") >= 18))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        (F.floor(F.col("premium") / 1000) * 1000).alias("premium_bucket"),
        F.date_trunc("week", F.col("end_date")).alias("end_date_week"),
    )
)

grouped = (
    valid
    .filter(F.col("product").isin("HEALTH", "LIFE"))
    .groupBy(
        F.date_trunc("month", "end_date_week").alias("end_date_week_month"),
        F.date_format("end_date", "yyyy-MM").alias("end_date_ym"),
    )
    .agg(
        F.min("premium").alias("min_premium"),
        F.round(F.sum("premium_bucket") / F.nullif(F.sum("premium"), F.lit(0)), 3).alias("premium_bucket_to_premium_ratio"),
        F.round(F.sum("premium"), 2).alias("total_premium"),
    )
)

filtered = (
    grouped
    .crossJoin(grouped.agg(F.min("total_premium").alias("overall_min_total_premium")))
    .filter(F.col("total_premium") > F.col("overall_min_total_premium"))
    .drop("overall_min_total_premium")
)

annotated = (
    filtered
    .select(
        "*",
        F.date_format(F.col("end_date_week_month"), "yyyy-MM-dd").alias("end_date_week_month_label"),
        F.greatest(F.col("min_premium"), F.col("total_premium")).alias("greatest_min_premium_total_premium"),
        F.upper(F.trim(F.col("end_date_ym"))).alias("end_date_ym_clean"),
    )
)

result = (
    annotated
    .select(
        "end_date_week_month",
        "end_date_ym",
        "premium_bucket_to_premium_ratio",
        "greatest_min_premium_total_premium",
        "min_premium",
        "total_premium",
    )
    .unpivot(
        ids=["end_date_week_month", "end_date_ym"],
        values=["premium_bucket_to_premium_ratio", "greatest_min_premium_total_premium", "min_premium", "total_premium"],
        variableColumnName="measure",
        valueColumnName="reading",
    )
)
