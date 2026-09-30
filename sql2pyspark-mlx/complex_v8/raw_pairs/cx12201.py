from pyspark.sql import functions as F

prepared = (
    df_policies
    .filter(F.col("status") == "ACTIVE")
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

summary = (
    prepared
    .groupBy(F.date_trunc("month", "end_date").alias("end_date_month"))
    .agg(
        F.count(F.when(~F.col("status").isin("LAPSED", "EXPIRED"), 1)).alias("n_status_not_in_lapsed_expired"),
        F.min("start_date").alias("first_start_date"),
        F.count("*").alias("n_rows"),
        F.sum("premium").alias("total_premium"),
        F.countDistinct("holder_id").alias("distinct_holder_ids"),
        F.max("start_date").alias("last_start_date"),
    )
)

filtered = (
    summary
    .crossJoin(summary.agg(F.min("n_rows").alias("overall_min_n_rows")))
    .filter(F.col("n_rows") > F.col("overall_min_n_rows"))
    .drop("overall_min_n_rows")
)

result = (
    filtered
    .select(
        "end_date_month",
        "n_status_not_in_lapsed_expired",
        "distinct_holder_ids",
        F.quarter(F.col("last_start_date")).alias("last_start_date_quarter"),
        F.round(F.col("total_premium") * 0.25, 2).alias("total_premium_adj"),
    )
    .orderBy("end_date_month")
)
