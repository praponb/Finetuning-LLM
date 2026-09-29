from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_product_holder_id = Window.partitionBy("product", "holder_id")
w_status_by_days_since_end_dat = Window.partitionBy("status").orderBy(F.col("days_since_end_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_start_date = Window.orderBy(F.col("start_date").desc(), "id")
w_all_by_start_date_prior_2 = Window.orderBy(F.col("start_date").desc(), "id").rowsBetween(-2, -1)
w_status_by_days_since_end_dat_2 = Window.partitionBy("status").orderBy(F.col("days_since_end_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_start_date_running = Window.orderBy(F.col("start_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_status_by_running_holder_ris = Window.partitionBy("status").orderBy(F.col("running_holder_risk_score").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_running_holder_risk_s = Window.orderBy("running_holder_risk_score", "id")
w_status_holder_id = Window.partitionBy("status", "holder_id")
w_status_by_running_holder_ris_2 = Window.partitionBy("status").orderBy(F.col("running_holder_risk_score").desc_nulls_first(), "id")

recent = (
    df_policies
    .filter((F.col("holder_id") >= 18) | (F.col("status").isin("EXPIRED", "ACTIVE", "CANCELLED")))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.round(F.col("premium") * 1.5, 2).alias("premium_adj"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("end_date")).alias("days_since_end_date"),
    )
)

combined_details_holder = (
    recent.alias("s")
    .join(df_policyholders.alias("p"), (F.col("p.id") == F.col("s.holder_id")) & (F.upper(F.trim(F.col("p.holder_name"))).like("A%")), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.start_date",
        "s.status",
        "s.premium_adj",
        "s.days_since_end_date",
        F.col("p.risk_score").alias("holder_risk_score"),
    )
)

metrics = (
    combined_details_holder
    .select(
        "*",
        F.round(F.col("premium_adj") / F.nullif(F.sum("premium_adj").over(w_product_holder_id), F.lit(0)), 4).alias("premium_adj_share_of_product_holder_id"),
        F.nth_value("premium_adj", 2).over(w_status_by_days_since_end_dat).alias("second_premium_adj"),
        F.round(F.percent_rank().over(w_all_by_start_date), 3).alias("start_date_percent_rank"),
        F.round(F.avg("days_since_end_date").over(w_all_by_start_date_prior_2), 2).alias("days_since_end_date_avg_2_prior"),
        F.round(F.cume_dist().over(w_all_by_start_date), 3).alias("start_date_cume_dist"),
        F.last("premium_adj", ignorenulls=True).over(w_status_by_days_since_end_dat_2).alias("last_known_premium_adj"),
        F.sum("holder_risk_score").over(w_all_by_start_date_running).alias("running_holder_risk_score"),
        (F.col("premium_adj") - F.lag("premium_adj").over(w_all_by_start_date)).alias("premium_adj_change"),
    )
)

labeled = (
    metrics
    .select(
        "id",
        "holder_id",
        "product",
        "status",
        "premium_adj",
        "days_since_end_date",
        "holder_risk_score",
        "start_date_percent_rank",
        "days_since_end_date_avg_2_prior",
        "start_date_cume_dist",
        "last_known_premium_adj",
        "running_holder_risk_score",
        "premium_adj_change",
        F.substring(F.trim(F.col("product")), 1, 4).alias("product_prefix"),
    )
)

metrics_2 = (
    labeled
    .select(
        "*",
        F.nth_value("holder_risk_score", 2).over(w_status_by_running_holder_ris).alias("second_holder_risk_score"),
        F.ntile(3).over(w_all_by_running_holder_risk_s).alias("running_holder_risk_score_tercile"),
        F.round(F.col("start_date_cume_dist") / F.nullif(F.sum("start_date_cume_dist").over(w_status_holder_id), F.lit(0)), 4).alias("start_date_cume_dist_share_of_status_holder_id"),
        F.min("premium_adj_change").over(w_status_holder_id).alias("status_holder_id_min_premium_adj_change"),
        F.round(F.cume_dist().over(w_status_by_running_holder_ris_2), 3).alias("running_holder_risk_score_cume_dist"),
    )
)

result = (
    metrics_2
    .select(
        "id",
        "product",
        "status",
        "premium_adj",
        "days_since_end_date",
        "holder_risk_score",
        "start_date_percent_rank",
        "start_date_cume_dist",
        "last_known_premium_adj",
        "running_holder_risk_score",
        "premium_adj_change",
        "product_prefix",
        "second_holder_risk_score",
        "running_holder_risk_score_cume_dist",
        F.concat_ws("|", F.col("status"), F.col("product")).alias("status_product_label"),
        F.coalesce(F.col("product"), F.lit("UNKNOWN")).alias("product_or_unknown"),
    )
    .orderBy("second_holder_risk_score", "premium_adj", "id")
    .limit(20)
)
