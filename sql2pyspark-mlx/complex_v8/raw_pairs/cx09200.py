from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_product_status_by_premium_ru = Window.partitionBy("product", "status").orderBy("premium", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_status_product = Window.partitionBy("status", "product")
w_holder_id_by_premium = Window.partitionBy("holder_id").orderBy("premium", "id")
w_holder_region_holder_id_by_h = Window.partitionBy("holder_region", "holder_id").orderBy(F.col("holder_risk_score").desc(), "id").rowsBetween(-6, Window.currentRow)
w_holder_id_by_premium_ties = Window.partitionBy("holder_id").orderBy("premium")
w_product_status_by_premium_fu = Window.partitionBy("product", "status").orderBy("premium", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_holder_id_by_premium_running = Window.partitionBy("holder_id").orderBy("premium", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_product_status_by_premium_pr = Window.partitionBy("product", "status").orderBy("premium", "id").rowsBetween(-6, -1)
w_distinct_holder_id_in_status = Window.partitionBy("distinct_holder_id_in_status_product", F.month("first_known_end_date")).orderBy(F.col("holder_risk_score").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_end_date_prior_6 = Window.orderBy("end_date", "id").rowsBetween(-6, -1)
w_holder_region = Window.partitionBy("holder_region")
w_n_rows = Window.partitionBy("n_rows")
w_all_by_n_rows = Window.orderBy("n_rows", "second_end_date_year")

base = (
    df_policies
    .filter(F.col("status").isNotNull())
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

enriched = (
    base.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.start_date",
        "s.end_date",
        "s.status",
        F.col("p.region").alias("holder_region"),
        "p.holder_name",
        F.col("p.risk_score").alias("holder_risk_score"),
    )
)

windowed = (
    enriched
    .select(
        "*",
        F.first("end_date", ignorenulls=True).over(w_product_status_by_premium_ru).alias("first_known_end_date"),
        F.size(F.collect_set("holder_id").over(w_status_product)).alias("distinct_holder_id_in_status_product"),
        F.datediff(F.col("end_date"), F.lag("end_date").over(w_holder_id_by_premium)).alias("days_since_prev_end_date"),
        F.round(F.avg("holder_risk_score").over(w_holder_region_holder_id_by_h), 2).alias("holder_risk_score_avg_7"),
        F.dense_rank().over(w_holder_id_by_premium_ties).alias("premium_dense_rank"),
        F.nth_value("end_date", 2).over(w_product_status_by_premium_fu).alias("second_end_date"),
        F.min("premium").over(w_product_status_by_premium_ru).alias("min_premium_so_far"),
        F.lead("end_date", 2).over(w_holder_id_by_premium).alias("next2_end_date"),
        F.round(F.percent_rank().over(w_holder_id_by_premium_ties), 3).alias("premium_percent_rank"),
        F.last("premium", ignorenulls=True).over(w_holder_id_by_premium_running).alias("last_known_premium"),
        F.max("holder_risk_score").over(w_product_status_by_premium_pr).alias("holder_risk_score_max_6_prior"),
        F.sum("holder_risk_score").over(w_status_product).alias("status_product_sum_holder_risk_score"),
    )
)

with_trends = (
    windowed
    .select(
        "id",
        "holder_id",
        "premium",
        "status",
        "holder_name",
        "holder_risk_score",
        "distinct_holder_id_in_status_product",
        "days_since_prev_end_date",
        "second_end_date",
        "min_premium_so_far",
        "premium_percent_rank",
        "last_known_premium",
        "status_product_sum_holder_risk_score",
        F.first("premium_percent_rank", ignorenulls=True).over(w_distinct_holder_id_in_status).alias("first_known_premium_percent_rank"),
        F.max("premium_dense_rank").over(w_all_by_end_date_prior_6).alias("premium_dense_rank_max_6_prior"),
        F.round(F.col("premium_dense_rank") / F.nullif(F.sum("premium_dense_rank").over(w_holder_region), F.lit(0)), 4).alias("premium_dense_rank_share_of_holder_region"),
    )
)

stats = (
    with_trends
    .filter(F.col("premium") < 3719.7)
    .groupBy(F.year("second_end_date").alias("second_end_date_year"))
    .agg(
        F.countDistinct("holder_id").alias("distinct_holder_ids"),
        F.count("*").alias("n_rows"),
        F.round(F.sum("min_premium_so_far") / F.nullif(F.sum("premium_percent_rank"), F.lit(0)), 3).alias("min_premium_so_far_to_premium_percent_rank_ratio"),
        F.sum(F.when(F.col("status") == "ACTIVE", F.col("holder_risk_score")).otherwise(0)).alias("holder_risk_score_status_active"),
    )
    .filter(F.col("n_rows") >= 2)
)

windowed_2 = (
    stats
    .select(
        "second_end_date_year",
        "distinct_holder_ids",
        "min_premium_so_far_to_premium_percent_rank_ratio",
        "holder_risk_score_status_active",
        F.round(F.col("second_end_date_year") - F.avg("second_end_date_year").over(w_n_rows), 2).alias("second_end_date_year_vs_n_rows_avg"),
        F.round(F.col("distinct_holder_ids") - F.avg("distinct_holder_ids").over(w_n_rows), 2).alias("distinct_holder_ids_vs_n_rows_avg"),
        F.lag("n_rows").over(w_all_by_n_rows).alias("prev_n_rows"),
        F.ntile(10).over(w_all_by_n_rows).alias("n_rows_decile"),
    )
)

result = (
    windowed_2
    .select(
        "second_end_date_year",
        "distinct_holder_ids",
        "holder_risk_score_status_active",
        "second_end_date_year_vs_n_rows_avg",
        "distinct_holder_ids_vs_n_rows_avg",
        "prev_n_rows",
        F.coalesce(F.col("distinct_holder_ids"), F.lit(0)).alias("distinct_holder_ids_filled"),
        F.round(F.col("distinct_holder_ids") / F.nullif(F.col("n_rows_decile"), F.lit(0)), 3).alias("distinct_holder_ids_per_n_rows_decile"),
    )
    .orderBy(
        F.col("second_end_date_year").asc_nulls_last(),
        F.col("distinct_holder_ids_vs_n_rows_avg").desc(),
    )
    .limit(20)
)
