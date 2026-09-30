from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_distinct_holder_ids_distinct = Window.partitionBy("distinct_holder_ids", "distinct_products").orderBy(F.col("distinct_products").desc(), "end_date_month").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_end_date_month_month = Window.partitionBy(F.date_trunc("month", "end_date_month"))
w_cnt_by_distinct_holder_ids_t = Window.partitionBy("cnt").orderBy(F.col("distinct_holder_ids").desc())
w_distinct_holder_ids_dense_ra = Window.partitionBy("distinct_holder_ids_dense_rank", "distinct_products").orderBy(F.col("distinct_holder_ids_dense_rank").desc(), "end_date_month").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_cnt = Window.partitionBy("cnt")
w_distinct_holder_ids_dense_ra_2 = Window.partitionBy("distinct_holder_ids_dense_rank", "distinct_products").orderBy(F.col("distinct_holder_ids_dense_rank").desc(), "end_date_month").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_distinct_holder_ids_dense_ra_3 = Window.partitionBy("distinct_holder_ids_dense_rank", "distinct_products").orderBy(F.col("distinct_holder_ids_dense_rank").desc(), "end_date_month")
w_last_known_distinct_holder_i = Window.partitionBy("last_known_distinct_holder_ids", "first_known_distinct_products")
w_cnt_distinct_holder_ids_dens = Window.partitionBy("cnt", "distinct_holder_ids_dense_rank").orderBy(F.col("pct_product_travel").desc(), "end_date_month")
w_last_known_distinct_holder_i_2 = Window.partitionBy("last_known_distinct_holder_ids").orderBy(F.col("distinct_holder_ids_vs_cnt_avg").desc(), "end_date_month").rowsBetween(-2, -1)

filtered_policys = (
    df_policies
    .filter((F.col("holder_id") >= 13) & (F.col("end_date").between(F.lit("2026-01-20").cast("date"), F.lit("2026-09-19").cast("date"))))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        F.length(F.trim(F.col("product"))).alias("product_len"),
        F.date_format(F.col("end_date"), "yyyy-MM-dd").alias("end_date_label"),
        F.coalesce(F.col("premium"), F.lit(0.0)).alias("premium_filled"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.count(F.when(F.col("claim_type") == "medical", 1)).alias("claim_claim_type_count"),
    )
)

summarized_rows = (
    filtered_policys.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_claim_type_count"), F.lit(0)).alias("claim_claim_type_count"),
    )
)

per_group = (
    summarized_rows
    .filter(F.col("start_date") >= F.lit("2024-05-07").cast("date"))
    .groupBy(F.date_trunc("month", "end_date").alias("end_date_month"))
    .agg(
        F.round(F.sum("premium") / F.nullif(F.sum("premium_filled"), F.lit(0)), 3).alias("premium_to_premium_filled_ratio"),
        F.countDistinct("holder_id").alias("distinct_holder_ids"),
        F.countDistinct("product").alias("distinct_products"),
        F.count("*").alias("cnt"),
        F.round(100 * F.count(F.when(F.col("product") != "TRAVEL", 1)) / F.count("*"), 1).alias("pct_product_travel"),
    )
)

with_windows = (
    per_group
    .select(
        "*",
        F.last("distinct_holder_ids", ignorenulls=True).over(w_distinct_holder_ids_distinct).alias("last_known_distinct_holder_ids"),
        F.count(F.lit(1)).over(w_end_date_month_month).alias("rows_in_end_date_month_month"),
        F.dense_rank().over(w_cnt_by_distinct_holder_ids_t).alias("distinct_holder_ids_dense_rank"),
    )
)

windowed = (
    with_windows
    .select(
        "*",
        F.last("end_date_month").over(w_distinct_holder_ids_dense_ra).alias("last_end_date_month"),
        F.first("distinct_products", ignorenulls=True).over(w_distinct_holder_ids_dense_ra_2).alias("first_known_distinct_products"),
        (F.col("cnt") - F.lag("cnt").over(w_distinct_holder_ids_dense_ra_3)).alias("cnt_change"),
        F.round(F.col("distinct_holder_ids") - F.avg("distinct_holder_ids").over(w_cnt), 2).alias("distinct_holder_ids_vs_cnt_avg"),
    )
)

with_trends = (
    windowed
    .select(
        "end_date_month",
        "premium_to_premium_filled_ratio",
        "distinct_holder_ids",
        "cnt",
        "pct_product_travel",
        "last_known_distinct_holder_ids",
        "rows_in_end_date_month_month",
        "last_end_date_month",
        "first_known_distinct_products",
        "cnt_change",
        "distinct_holder_ids_vs_cnt_avg",
        F.sum("rows_in_end_date_month_month").over(w_last_known_distinct_holder_i).alias("last_known_distinct_holder_ids_first_known_disti"),
        F.lead("distinct_products").over(w_cnt_distinct_holder_ids_dens).alias("next_distinct_products"),
        F.sum("cnt_change").over(w_last_known_distinct_holder_i_2).alias("cnt_change_sum_2_prior"),
    )
)

labeled = (
    with_trends
    .select(
        "*",
        F.coalesce(F.col("distinct_holder_ids"), F.lit(0)).alias("distinct_holder_ids_filled"),
        F.round(F.col("distinct_holder_ids") / F.nullif(F.col("cnt_change"), F.lit(0)), 3).alias("distinct_holder_ids_per_cnt_change"),
    )
)

result = (
    labeled
    .filter(F.col("last_known_distinct_holder_ids") > 4)
    .select(
        "end_date_month",
        "distinct_holder_ids",
        "cnt",
        "pct_product_travel",
        "rows_in_end_date_month_month",
        "first_known_distinct_products",
        "cnt_change",
        "distinct_holder_ids_vs_cnt_avg",
        "last_known_distinct_holder_ids_first_known_disti",
        "next_distinct_products",
        "cnt_change_sum_2_prior",
        "distinct_holder_ids_filled",
    )
    .orderBy("pct_product_travel", F.col("end_date_month").desc_nulls_first())
    .limit(20)
)
