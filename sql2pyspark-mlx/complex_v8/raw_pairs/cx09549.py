from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_product_by_days_since_start = Window.partitionBy("product").orderBy("days_since_start_date", "id")
w_end_date_month_status = Window.partitionBy(F.month("end_date"), "status")
w_product_by_days_since_start_2 = Window.partitionBy("product").orderBy("days_since_start_date")

scoped = (
    df_policies
    .filter((F.col("status").isin("LAPSED", "ACTIVE")) & (F.col("start_date").between(F.lit("2024-05-07").cast("date"), F.lit("2025-07-07").cast("date"))))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("start_date")).alias("days_since_start_date"),
        F.date_format(F.col("start_date"), "yyyy-MM").alias("start_date_label"),
    )
)

metrics = (
    scoped
    .select(
        "*",
        F.lag("end_date").over(w_product_by_days_since_start).alias("prev_end_date"),
        F.size(F.collect_set("product").over(w_end_date_month_status)).alias("distinct_product_in_end_date_month_status"),
        F.round(F.percent_rank().over(w_product_by_days_since_start_2), 3).alias("days_since_start_date_percent_rank"),
        F.size(F.collect_set("holder_id").over(w_end_date_month_status)).alias("distinct_holder_id_in_end_date_month_status"),
    )
)

without_matches = (
    metrics.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.col("c.approved_amount") <= 21750.2) & (F.to_date(F.col("c.reported_at")) < F.lit("2025-02-12").cast("date")), "left_semi")
)

result = (
    without_matches
    .groupingSets(
        [["distinct_holder_id_in_end_date_month_status", "distinct_product_in_end_date_month_status"], ["distinct_holder_id_in_end_date_month_status"], ["distinct_product_in_end_date_month_status"], []],
        "distinct_holder_id_in_end_date_month_status", "distinct_product_in_end_date_month_status",
    )
    .agg(
        F.grouping("distinct_holder_id_in_end_date_month_status").alias("is_total_distinct_holder_id_in_end_date_month_st"),
        F.min("prev_end_date").alias("first_prev_end_date"),
        F.percentile_approx("days_since_start_date_percent_rank", 0.5).alias("median_days_since_start_date_percent_rank"),
    )
    .orderBy(
        F.col("distinct_holder_id_in_end_date_month_status").asc_nulls_last(),
        F.col("distinct_product_in_end_date_month_status").asc_nulls_last(),
    )
)
