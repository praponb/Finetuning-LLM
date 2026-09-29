from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_product = Window.partitionBy("product")
w_all_by_n_status_in_active_ca = Window.orderBy(F.col("n_status_in_active_cancelled_l").desc(), "status", "end_date_year", "product").rowsBetween(-2, -1)
w_end_date_year_distinct_holde = Window.partitionBy("end_date_year", "distinct_holder_ids").orderBy(F.col("pct_product_health").desc())
w_distinct_holder_ids = Window.partitionBy("distinct_holder_ids")

recent_policys = (
    df_policies
    .filter((F.col("premium") >= 2111.3) & (F.col("start_date") < F.lit("2025-05-02").cast("date")))
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "end_date",
        "status",
        F.substring(F.trim(F.col("product")), 1, 4).alias("product_prefix"),
        F.date_format(F.col("start_date"), "yyyy-MM").alias("start_date_label"),
    )
)

grouped = (
    recent_policys
    .filter(F.col("holder_id") <= 11)
    .groupBy("status", F.year("end_date").alias("end_date_year"), "product")
    .agg(
        F.round(100 * F.count(F.when(F.col("product") == "HEALTH", 1)) / F.count("*"), 1).alias("pct_product_health"),
        F.countDistinct("holder_id").alias("distinct_holder_ids"),
        F.count(F.when(F.col("status").isin("ACTIVE", "CANCELLED", "LAPSED"), 1)).alias("n_status_in_active_cancelled_l"),
        F.max("end_date").alias("last_end_date"),
    )
)

without_matches = (
    grouped
    .crossJoin(grouped.agg(F.min("n_status_in_active_cancelled_l").alias("overall_min_n_status_in_active_cancelled_l")))
    .filter(F.col("n_status_in_active_cancelled_l") > F.col("overall_min_n_status_in_active_cancelled_l"))
    .drop("overall_min_n_status_in_active_cancelled_l")
)

derived = (
    without_matches
    .filter(F.col("n_status_in_active_cancelled_l") >= 2)
    .select(
        "*",
        F.date_format(F.col("last_end_date"), "yyyy-MM").alias("last_end_date_label"),
        (F.col("distinct_holder_ids") + F.col("end_date_year")).alias("distinct_holder_ids_plus_end_date_year"),
        F.coalesce(F.col("distinct_holder_ids"), F.lit(0)).alias("distinct_holder_ids_filled"),
    )
)

with_trends = (
    derived
    .select(
        "*",
        F.count(F.lit(1)).over(w_product).alias("rows_in_product"),
        F.round(F.col("pct_product_health") - F.avg("pct_product_health").over(w_product), 2).alias("pct_product_health_vs_product_avg"),
        F.round(F.avg("distinct_holder_ids_filled").over(w_all_by_n_status_in_active_ca), 2).alias("distinct_holder_ids_filled_avg_2_prior"),
    )
)

with_activity = (
    with_trends
    .crossJoin(with_trends.agg(F.avg("pct_product_health_vs_product_avg").alias("overall_avg_pct_product_health_vs_product_avg")))
    .filter(F.col("pct_product_health_vs_product_avg") > F.col("overall_avg_pct_product_health_vs_product_avg"))
    .drop("overall_avg_pct_product_health_vs_product_avg")
)

metrics = (
    with_activity
    .withColumn("pct_product_health_percent_rank", F.round(F.percent_rank().over(w_end_date_year_distinct_holde), 3))
    .withColumn("distinct_holder_ids_min_rows_in_product", F.min("rows_in_product").over(w_distinct_holder_ids))
)

result = (
    metrics
    .filter(F.col("last_end_date_label").isNotNull())
    .select(
        "status",
        "end_date_year",
        "pct_product_health",
        "distinct_holder_ids",
        "n_status_in_active_cancelled_l",
        "last_end_date_label",
        "distinct_holder_ids_plus_end_date_year",
        "distinct_holder_ids_filled",
        "rows_in_product",
        "pct_product_health_vs_product_avg",
        "pct_product_health_percent_rank",
        "distinct_holder_ids_min_rows_in_product",
        F.least(F.col("rows_in_product"), F.col("pct_product_health")).alias("least_rows_in_product_pct_product_health"),
        F.coalesce(F.col("status"), F.lit("UNKNOWN")).alias("status_or_unknown"),
    )
)
