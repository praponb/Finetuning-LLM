from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_end_date_prior_2 = Window.orderBy("end_date", "id").rowsBetween(-2, -1)
w_product = Window.partitionBy("product")
w_status = Window.partitionBy("status")
w_all_by_end_date_running = Window.orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_end_date_full = Window.orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_end_date_ties = Window.orderBy("end_date")
w_distinct_product_in_status = Window.partitionBy("distinct_product_in_status")
w_all_by_premium_vs_product_av = Window.orderBy(F.col("premium_vs_product_avg_filled").desc_nulls_first(), "id")

cleaned_policys = (
    df_policies
    .filter((F.col("start_date") >= F.lit("2024-08-27").cast("date")) & (F.col("end_date").between(F.lit("2025-09-27").cast("date"), F.lit("2026-10-23").cast("date"))))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.add_months(F.col("start_date"), 12).alias("start_date_plus_12m"),
        F.lower(F.regexp_replace(F.trim(F.col("status")), " ", "_")).alias("status_slug"),
    )
)

sequenced = (
    cleaned_policys
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "end_date",
        "status",
        "status_slug",
        F.max("premium").over(w_all_by_end_date_prior_2).alias("premium_max_2_prior"),
        F.size(F.collect_set("status").over(w_product)).alias("distinct_status_in_product"),
        F.round(F.col("premium") - F.avg("premium").over(w_product), 2).alias("premium_vs_product_avg"),
        F.size(F.collect_set("product").over(w_status)).alias("distinct_product_in_status"),
        F.first("premium", ignorenulls=True).over(w_all_by_end_date_running).alias("first_known_premium"),
        F.last("end_date").over(w_all_by_end_date_full).alias("last_end_date"),
        F.last("end_date", ignorenulls=True).over(w_all_by_end_date_running).alias("last_known_end_date"),
        F.dense_rank().over(w_all_by_end_date_ties).alias("end_date_dense_rank"),
    )
)

labeled = (
    sequenced
    .select(
        "*",
        F.coalesce(F.col("premium_vs_product_avg"), F.lit(0.0)).alias("premium_vs_product_avg_filled"),
        F.round(F.col("first_known_premium") * 1.5, 2).alias("first_known_premium_adj"),
    )
)

with_trends = (
    labeled
    .select(
        "id",
        "holder_id",
        "premium",
        "end_date",
        "status",
        "premium_max_2_prior",
        "distinct_status_in_product",
        "last_end_date",
        "last_known_end_date",
        "end_date_dense_rank",
        "premium_vs_product_avg_filled",
        F.sum("first_known_premium").over(w_distinct_product_in_status).alias("distinct_product_in_status_sum_first_known_premi"),
        F.round(F.avg("first_known_premium").over(w_distinct_product_in_status), 2).alias("distinct_product_in_status_avg_first_known_premi"),
        F.lag("distinct_status_in_product").over(w_all_by_premium_vs_product_av).alias("prev_distinct_status_in_product"),
    )
)

result = (
    with_trends
    .filter(F.col("holder_id").isNotNull())
    .select(
        "id",
        "holder_id",
        "premium",
        "end_date",
        "status",
        "premium_max_2_prior",
        "last_end_date",
        "last_known_end_date",
        "end_date_dense_rank",
        "premium_vs_product_avg_filled",
        "distinct_product_in_status_sum_first_known_premi",
        "distinct_product_in_status_avg_first_known_premi",
        "prev_distinct_status_in_product",
        F.abs(F.col("end_date_dense_rank") - 6).alias("end_date_dense_rank_distance"),
        F.substring(F.trim(F.col("status")), 1, 4).alias("status_prefix"),
    )
    .orderBy(F.col("premium").desc())
)
