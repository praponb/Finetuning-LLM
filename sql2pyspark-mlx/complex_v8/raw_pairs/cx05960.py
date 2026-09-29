from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_distinct_holder_ids_by_avg_h = Window.partitionBy("distinct_holder_ids").orderBy(F.col("avg_holder_risk_score").asc_nulls_last(), "end_date_month", "product", "holder_joined_on_month").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_end_date_month_year_by_media = Window.partitionBy(F.year("end_date_month")).orderBy(F.col("median_holder_risk_score").asc_nulls_last(), "end_date_month", "product", "holder_joined_on_month").rowsBetween(-1, Window.currentRow)
w_distinct_holder_ids_by_avg_h_2 = Window.partitionBy("distinct_holder_ids").orderBy(F.col("avg_holder_risk_score").asc_nulls_last())
w_all_by_min_holder_risk_score = Window.orderBy("min_holder_risk_score", "end_date_month", "product", "holder_joined_on_month").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_min_holder_risk_score_2 = Window.orderBy("min_holder_risk_score", "end_date_month", "product", "holder_joined_on_month")
w_all_by_min_holder_risk_score_3 = Window.orderBy("min_holder_risk_score")
w_product_distinct_holder_ids = Window.partitionBy("product", "distinct_holder_ids").orderBy(F.col("min_holder_risk_score_avg_2").desc_nulls_first(), "end_date_month", "product", "holder_joined_on_month").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_product_distinct_holder_ids_2 = Window.partitionBy("product", "distinct_holder_ids").orderBy(F.col("min_holder_risk_score_avg_2").desc_nulls_first())
w_product_distinct_holder_ids_3 = Window.partitionBy("product", "distinct_holder_ids").orderBy(F.col("min_holder_risk_score_avg_2").desc_nulls_first(), "end_date_month", "product", "holder_joined_on_month")
w_first_distinct_holder_ids = Window.partitionBy("first_distinct_holder_ids")

filtered_policys = (
    df_policies
    .filter((F.col("premium") >= 1042.8) | (~F.col("product").isin("HEALTH", "AUTO")))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("end_date")).alias("days_since_end_date"),
        F.datediff(F.col("end_date"), F.col("start_date")).alias("days_start_date_to_end_date"),
    )
)

enriched = (
    filtered_policys.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.end_date",
        "s.days_since_end_date",
        "p.holder_name",
        F.col("p.risk_score").alias("holder_risk_score"),
        F.col("p.joined_on").alias("holder_joined_on"),
    )
)

stats = (
    enriched
    .filter(F.col("holder_risk_score") > 0.2)
    .groupBy(
        F.date_trunc("month", "end_date").alias("end_date_month"),
        "product",
        F.date_trunc("month", "holder_joined_on").alias("holder_joined_on_month"),
    )
    .agg(
        F.min("holder_risk_score").alias("min_holder_risk_score"),
        F.round(F.avg("holder_risk_score"), 2).alias("avg_holder_risk_score"),
        F.countDistinct("holder_id").alias("distinct_holder_ids"),
        F.sum("holder_risk_score").alias("total_holder_risk_score"),
        F.percentile_approx("holder_risk_score", 0.5).alias("median_holder_risk_score"),
    )
)

metrics = (
    stats
    .select(
        "*",
        F.first("distinct_holder_ids").over(w_distinct_holder_ids_by_avg_h).alias("first_distinct_holder_ids"),
        F.round(F.avg("min_holder_risk_score").over(w_end_date_month_year_by_media), 2).alias("min_holder_risk_score_avg_2"),
        F.round(F.cume_dist().over(w_distinct_holder_ids_by_avg_h_2), 3).alias("avg_holder_risk_score_cume_dist"),
    )
)

metrics_2 = (
    metrics
    .select(
        "*",
        F.nth_value("first_distinct_holder_ids", 2).over(w_all_by_min_holder_risk_score).alias("second_first_distinct_holder_ids"),
        F.rank().over(w_all_by_min_holder_risk_score_2).alias("min_holder_risk_score_rank"),
        F.round(F.percent_rank().over(w_all_by_min_holder_risk_score_3), 3).alias("min_holder_risk_score_percent_rank"),
    )
)

with_trends = (
    metrics_2
    .select(
        "end_date_month",
        "product",
        "holder_joined_on_month",
        "min_holder_risk_score",
        "avg_holder_risk_score",
        "distinct_holder_ids",
        "median_holder_risk_score",
        "first_distinct_holder_ids",
        "avg_holder_risk_score_cume_dist",
        "second_first_distinct_holder_ids",
        "min_holder_risk_score_rank",
        "min_holder_risk_score_percent_rank",
        F.nth_value("min_holder_risk_score_avg_2", 2).over(w_product_distinct_holder_ids).alias("second_min_holder_risk_score_avg_2"),
        F.rank().over(w_product_distinct_holder_ids_2).alias("min_holder_risk_score_avg_2_rank"),
        F.ntile(3).over(w_product_distinct_holder_ids_3).alias("min_holder_risk_score_avg_2_tercile"),
        F.max("min_holder_risk_score_avg_2").over(w_first_distinct_holder_ids).alias("first_distinct_holder_ids_max_min_holder_risk_sc"),
    )
)

result = (
    with_trends
    .rollup("min_holder_risk_score_avg_2_rank", "distinct_holder_ids", "min_holder_risk_score_rank")
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.round(F.stddev("first_distinct_holder_ids_max_min_holder_risk_sc"), 2).alias("stddev_first_distinct_holder_ids_max_min_holder_"),
        F.sum(F.when(F.col("product") != "AUTO", F.col("median_holder_risk_score"))).alias("median_holder_risk_score_product_auto"),
        F.round(F.sum("first_distinct_holder_ids") / F.nullif(F.sum("min_holder_risk_score"), F.lit(0)), 3).alias("first_distinct_holder_ids_to_min_holder_risk_sco"),
    )
    .orderBy(
        F.col("min_holder_risk_score_avg_2_rank").asc_nulls_last(),
        F.col("distinct_holder_ids").asc_nulls_last(),
        F.col("min_holder_risk_score_rank").asc_nulls_last(),
    )
)
