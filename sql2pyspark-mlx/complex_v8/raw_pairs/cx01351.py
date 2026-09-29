from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status = Window.partitionBy("status")
w_holder_region_product_by_sta = Window.partitionBy("holder_region", "product").orderBy(F.col("start_date").desc(), "id").rowsBetween(-2, -1)
w_status_by_premium_running = Window.partitionBy("status").orderBy(F.col("premium").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_holder_region_status_by_hold = Window.partitionBy("holder_region", "status").orderBy(F.col("holder_joined_on").desc(), "id")
w_holder_region_product_by_sta_2 = Window.partitionBy("holder_region", "product").orderBy(F.col("start_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_holder_region_status_by_hold_2 = Window.partitionBy("holder_region", "status").orderBy(F.col("holder_joined_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_holder_region_product_by_sta_3 = Window.partitionBy("holder_region", "product").orderBy(F.col("start_date").desc(), "id")

cleaned = (
    df_policies
    .filter(F.col("start_date") >= F.lit("2024-07-19").cast("date"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.concat_ws(" / ", F.col("status"), F.col("product")).alias("status_product_label"),
        F.date_trunc("month", F.col("start_date")).alias("start_date_month"),
    )
)

combined_details_holder = (
    cleaned.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.*",
        F.col("p.joined_on").alias("holder_joined_on"),
        F.col("p.risk_score").alias("holder_risk_score"),
        F.col("p.region").alias("holder_region"),
    )
)

with_windows = (
    combined_details_holder
    .select(
        "*",
        F.round(F.col("holder_risk_score") / F.nullif(F.sum("holder_risk_score").over(w_status), F.lit(0)), 4).alias("holder_risk_score_share_of_status"),
        F.sum("premium").over(w_holder_region_product_by_sta).alias("premium_sum_2_prior"),
        F.min("holder_risk_score").over(w_status_by_premium_running).alias("min_holder_risk_score_so_far"),
        F.dense_rank().over(w_holder_region_status_by_hold).alias("holder_joined_on_dense_rank"),
        F.size(F.collect_set("product").over(w_status)).alias("distinct_product_in_status"),
        F.count(F.lit(1)).over(w_holder_region_product_by_sta_2).alias("running_count"),
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_status), F.lit(0)), 4).alias("premium_share_of_status"),
        F.nth_value("holder_risk_score", 2).over(w_holder_region_status_by_hold_2).alias("second_holder_risk_score"),
        F.count(F.lit(1)).over(w_status).alias("rows_in_status"),
        F.sum("premium").over(w_status).alias("status_sum_premium"),
        F.round(F.percent_rank().over(w_holder_region_product_by_sta_3), 3).alias("start_date_percent_rank"),
    )
)

result = (
    with_windows
    .filter(F.col("holder_region").isNotNull())
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "start_date_month",
        "holder_risk_score",
        "holder_risk_score_share_of_status",
        "premium_sum_2_prior",
        "min_holder_risk_score_so_far",
        "distinct_product_in_status",
        "premium_share_of_status",
        "second_holder_risk_score",
        "rows_in_status",
        "start_date_percent_rank",
        F.when(F.col("holder_risk_score") >= 0.6, "HIGH").when(F.col("holder_risk_score") >= 0.2, "MEDIUM").otherwise("LOW").alias("holder_risk_score_tier"),
    )
    .orderBy(
        "start_date_month",
        F.col("distinct_product_in_status").asc_nulls_last(),
    )
)
