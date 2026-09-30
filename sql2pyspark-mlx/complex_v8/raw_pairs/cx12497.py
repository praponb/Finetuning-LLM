from pyspark.sql import functions as F

prepared = (
    df_policies
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "status",
        F.quarter(F.col("start_date")).alias("start_date_quarter"),
        F.date_format(F.col("start_date"), "yyyy-MM").alias("start_date_label"),
        F.coalesce(F.col("status"), F.lit("UNKNOWN")).alias("status_or_unknown"),
    )
)

derived = (
    prepared
    .select(
        "*",
        F.round(F.col("start_date_quarter") / F.nullif(F.col("premium"), F.lit(0)), 3).alias("start_date_quarter_per_premium"),
        F.coalesce(F.col("start_date_quarter"), F.lit(0)).alias("start_date_quarter_filled"),
    )
)

stats = (
    derived
    .groupBy(
        F.when(F.col("premium") >= 3532.7, "A").when(F.col("premium") >= 2013.8, "B").otherwise("C").alias("premium_tier"),
        "holder_id",
        "status",
    )
    .agg(
        F.max("premium").alias("max_premium"),
        F.round(F.avg("start_date_quarter_filled"), 2).alias("avg_start_date_quarter_filled"),
        F.count(F.when(F.col("status") != "ACTIVE", 1)).alias("n_status_active"),
        F.max("start_date").alias("last_start_date"),
        F.count(F.when(F.col("status").isNotNull(), 1)).alias("n_status_is_not_null"),
    )
)

scored = (
    stats
    .select(
        "premium_tier",
        "holder_id",
        "status",
        "max_premium",
        "avg_start_date_quarter_filled",
        "n_status_active",
        "n_status_is_not_null",
        F.when(F.col("max_premium") >= 3320.0, "A").when(F.col("max_premium") >= 2120.0, "B").otherwise("C").alias("max_premium_tier"),
    )
)

result = (
    scored
    .select(
        "premium_tier",
        "holder_id",
        "avg_start_date_quarter_filled",
        "max_premium",
    )
    .unpivot(
        ids=["premium_tier", "holder_id"],
        values=["avg_start_date_quarter_filled", "max_premium"],
        variableColumnName="metric",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
