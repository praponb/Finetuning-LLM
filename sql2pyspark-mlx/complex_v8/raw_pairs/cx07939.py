from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_holder_risk_score_run = Window.orderBy("holder_risk_score", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_holder_risk_score = Window.orderBy(F.col("holder_risk_score").desc(), "id")
w_all_by_holder_risk_score_tie = Window.orderBy(F.col("holder_risk_score").desc())
w_holder_region_by_holder_risk = Window.partitionBy("holder_region").orderBy(F.col("holder_risk_score").desc(), "id")
w_end_date_month = Window.partitionBy(F.date_trunc("month", "end_date"))
w_all_by_holder_risk_score_run_2 = Window.orderBy(F.col("holder_risk_score").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_holder_region_holder_id = Window.partitionBy("holder_region", "holder_id")
w_last_known_end_date_month_pr = Window.partitionBy(F.date_trunc("month", "last_known_end_date"), "product")
w_product_holder_risk_score_ra = Window.partitionBy("product", "holder_risk_score_rank_2").orderBy("last_known_holder_risk_score", "id")
w_product = Window.partitionBy("product")
w_product_len_holder_region_or = Window.partitionBy("product_len", "holder_region_or_unknown").orderBy(F.col("holder_risk_score_rank_2").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_product_len_holder_region_or_2 = Window.partitionBy("product_len", "holder_region_or_unknown").orderBy(F.col("holder_risk_score_rank_2").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_product_holder_risk_score_ra_2 = Window.partitionBy("product", "holder_risk_score_rank_2").orderBy("last_known_holder_risk_score", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

base = (
    df_policies
    .filter(F.col("end_date").between(F.lit("2025-12-05").cast("date"), F.lit("2026-09-23").cast("date")))
)

enriched_holder = (
    base.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.end_date",
        F.col("p.risk_score").alias("holder_risk_score"),
        F.col("p.region").alias("holder_region"),
    )
)

with_trends = (
    enriched_holder
    .select(
        "*",
        F.last("end_date", ignorenulls=True).over(w_all_by_holder_risk_score_run).alias("last_known_end_date"),
        F.rank().over(w_all_by_holder_risk_score).alias("holder_risk_score_rank"),
        F.rank().over(w_all_by_holder_risk_score_tie).alias("holder_risk_score_rank_2"),
        F.lead("end_date").over(w_holder_region_by_holder_risk).alias("next_end_date"),
        F.size(F.collect_set("product").over(w_end_date_month)).alias("distinct_product_in_end_date_month"),
        F.last("holder_risk_score", ignorenulls=True).over(w_all_by_holder_risk_score_run_2).alias("last_known_holder_risk_score"),
        F.size(F.collect_set("product").over(w_holder_region_holder_id)).alias("distinct_product_in_holder_region_holder_id"),
        F.max("holder_risk_score").over(w_holder_region_holder_id).alias("holder_region_holder_id_max_holder_risk_score"),
        F.round(F.col("holder_risk_score") - F.avg("holder_risk_score").over(w_end_date_month), 2).alias("holder_risk_score_vs_end_date_month_avg"),
        F.round(F.col("holder_risk_score") / F.nullif(F.sum("holder_risk_score").over(w_holder_region_holder_id), F.lit(0)), 4).alias("holder_risk_score_share_of_holder_region_holder_"),
        F.first("holder_risk_score", ignorenulls=True).over(w_all_by_holder_risk_score_run_2).alias("first_known_holder_risk_score"),
        F.round(F.percent_rank().over(w_all_by_holder_risk_score_tie), 3).alias("holder_risk_score_percent_rank"),
    )
)

enriched = (
    with_trends
    .filter(F.col("last_known_end_date") >= F.lit("2026-01-24").cast("date"))
    .select(
        "id",
        "product",
        "end_date",
        "last_known_end_date",
        "holder_risk_score_rank_2",
        "next_end_date",
        "distinct_product_in_end_date_month",
        "last_known_holder_risk_score",
        "distinct_product_in_holder_region_holder_id",
        "holder_risk_score_vs_end_date_month_avg",
        "holder_risk_score_share_of_holder_region_holder_",
        "first_known_holder_risk_score",
        F.coalesce(F.col("holder_region"), F.lit("UNKNOWN")).alias("holder_region_or_unknown"),
        (F.floor(F.col("holder_risk_score_rank_2") / 2) * 2).alias("holder_risk_score_rank_2_bucket"),
        F.length(F.trim(F.col("product"))).alias("product_len"),
    )
)

ranked = (
    enriched
    .select(
        "*",
        F.round(F.col("last_known_holder_risk_score") - F.avg("last_known_holder_risk_score").over(w_last_known_end_date_month_pr), 2).alias("last_known_holder_risk_score_vs_last_known_end_d"),
        F.lead("next_end_date").over(w_product_holder_risk_score_ra).alias("next_next_end_date"),
        F.min("holder_risk_score_rank_2").over(w_product).alias("product_min_holder_risk_score_rank_2"),
        F.last("end_date", ignorenulls=True).over(w_product_len_holder_region_or).alias("last_known_end_date_2"),
        F.nth_value("last_known_end_date", 2).over(w_product_len_holder_region_or_2).alias("second_last_known_end_date"),
        F.round(F.col("holder_risk_score_rank_2") - F.avg("holder_risk_score_rank_2").over(w_product), 2).alias("holder_risk_score_rank_2_vs_product_avg"),
        F.last("last_known_holder_risk_score").over(w_product_holder_risk_score_ra_2).alias("last_last_known_holder_risk_score"),
    )
)

result = (
    ranked
    .select(
        "id",
        "product",
        "end_date",
        "last_known_end_date",
        "holder_risk_score_rank_2",
        "distinct_product_in_end_date_month",
        "last_known_holder_risk_score",
        "distinct_product_in_holder_region_holder_id",
        "holder_risk_score_vs_end_date_month_avg",
        "holder_risk_score_share_of_holder_region_holder_",
        "first_known_holder_risk_score",
        "holder_region_or_unknown",
        "holder_risk_score_rank_2_bucket",
        "product_len",
        "next_next_end_date",
        "product_min_holder_risk_score_rank_2",
        "holder_risk_score_rank_2_vs_product_avg",
        "last_last_known_holder_risk_score",
    )
    .orderBy(F.col("last_last_known_holder_risk_score").desc(), "id")
    .limit(5)
)
