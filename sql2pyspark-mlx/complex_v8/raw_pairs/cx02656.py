from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_n_rows_distinct_ids_by_end_d = Window.partitionBy("n_rows", "distinct_ids").orderBy("end_date_span_days", "product", "start_date_month", "end_date_ym").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_n_rows_distinct_ids_by_end_d_2 = Window.partitionBy("n_rows", "distinct_ids").orderBy("end_date_span_days", "product", "start_date_month", "end_date_ym")
w_n_rows_distinct_ids_by_end_d_3 = Window.partitionBy("n_rows", "distinct_ids").orderBy("end_date_span_days", "product", "start_date_month", "end_date_ym").rowsBetween(-2, -1)
w_n_rows_distinct_ids_by_end_d_4 = Window.partitionBy("n_rows", "distinct_ids").orderBy("end_date_span_days", "product", "start_date_month", "end_date_ym").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

valid = (
    df_policies
    .filter((F.col("start_date") < F.lit("2025-07-07").cast("date")) & (F.year(F.col("end_date")) == 2026))
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

filtered = (
    valid.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.col("c.adjuster_id").isNotNull()) & (F.col("c.approved_amount") < 14595.6), "left_anti")
)

totals = (
    filtered
    .groupBy(
        "product",
        F.date_trunc("month", "start_date").alias("start_date_month"),
        F.date_format("end_date", "yyyy-MM").alias("end_date_ym"),
    )
    .agg(
        F.round(F.sum("premium"), 2).alias("total_premium"),
        F.datediff(F.max("end_date"), F.min("end_date")).alias("end_date_span_days"),
        F.count(F.when(F.col("product") == "LIFE", 1)).alias("n_product_life"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("n_rows"),
        F.sum("premium").alias("total_premium_2"),
    )
    .filter(F.col("n_rows") >= 2)
)

sequenced = (
    totals
    .select(
        "product",
        "start_date_month",
        "end_date_ym",
        "total_premium",
        "end_date_span_days",
        "n_product_life",
        F.last("distinct_ids", ignorenulls=True).over(w_n_rows_distinct_ids_by_end_d).alias("last_known_distinct_ids"),
        F.ntile(4).over(w_n_rows_distinct_ids_by_end_d_2).alias("end_date_span_days_quartile"),
        F.round(F.percent_rank().over(w_n_rows_distinct_ids_by_end_d_2), 3).alias("end_date_span_days_percent_rank"),
        F.sum("distinct_ids").over(w_n_rows_distinct_ids_by_end_d_3).alias("distinct_ids_sum_2_prior"),
        F.nth_value("total_premium", 2).over(w_n_rows_distinct_ids_by_end_d_4).alias("second_total_premium"),
        F.last("start_date_month").over(w_n_rows_distinct_ids_by_end_d_4).alias("last_start_date_month"),
    )
)

without_matches = (
    sequenced
    .crossJoin(sequenced.agg(F.max("distinct_ids_sum_2_prior").alias("overall_max_distinct_ids_sum_2_prior")))
    .filter(F.col("distinct_ids_sum_2_prior") < F.col("overall_max_distinct_ids_sum_2_prior"))
    .drop("overall_max_distinct_ids_sum_2_prior")
)

enriched = (
    without_matches
    .select(
        "*",
        F.round((F.unix_timestamp(F.col("last_start_date_month")) - F.unix_timestamp(F.col("start_date_month"))) / 3600, 1).alias("hours_start_date_month_to_last_start_date_month"),
    )
)

without_matches_2 = (
    enriched
    .crossJoin(enriched.agg(F.avg("end_date_span_days").alias("overall_avg_end_date_span_days")))
    .filter(F.col("end_date_span_days") > F.col("overall_avg_end_date_span_days"))
    .drop("overall_avg_end_date_span_days")
)

result = (
    without_matches_2
    .select(
        "product",
        "start_date_month",
        "second_total_premium",
        "hours_start_date_month_to_last_start_date_month",
    )
    .unpivot(
        ids=["product", "start_date_month"],
        values=["second_total_premium", "hours_start_date_month_to_last_start_date_month"],
        variableColumnName="measure",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
