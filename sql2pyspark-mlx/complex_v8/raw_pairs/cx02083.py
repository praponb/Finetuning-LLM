from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_id = Window.partitionBy("holder_id")
w_all_by_premium_running = Window.orderBy("premium", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_product_status_by_premium = Window.partitionBy("product", "status").orderBy(F.col("premium").desc(), "id")
w_all_by_premium_prior_6 = Window.orderBy("premium", "id").rowsBetween(-6, -1)

recent = (
    df_policies
    .filter(F.col("product").isin("HOME", "LIFE"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "status",
        F.coalesce(F.col("status"), F.lit("UNKNOWN")).alias("status_or_unknown"),
        (F.floor(F.col("premium") / 1000) * 1000).alias("premium_bucket"),
        F.datediff(F.col("end_date"), F.col("start_date")).alias("days_start_date_to_end_date"),
    )
)

sequenced = (
    recent
    .select(
        "*",
        F.sum("premium").over(w_holder_id).alias("holder_id_sum_premium"),
        F.sum("premium_bucket").over(w_all_by_premium_running).alias("running_premium_bucket"),
        F.rank().over(w_product_status_by_premium).alias("premium_rank"),
        F.max("premium_bucket").over(w_all_by_premium_prior_6).alias("premium_bucket_max_6_prior"),
        F.max("premium_bucket").over(w_holder_id).alias("holder_id_max_premium_bucket"),
        F.min("premium").over(w_holder_id).alias("holder_id_min_premium"),
    )
)

status_or_unknown_stats = (
    sequenced
    .groupBy("status_or_unknown")
    .agg(F.avg("running_premium_bucket").alias("avg_running_premium_bucket"))
)

above_average = (
    sequenced.alias("s")
    .join(status_or_unknown_stats.alias("g"), F.col("g.status_or_unknown") == F.col("s.status_or_unknown"), "inner")
    .filter(F.col("s.running_premium_bucket") > F.col("g.avg_running_premium_bucket"))
    .select(
        "s.*",
        "g.avg_running_premium_bucket",
        F.round(F.col("s.running_premium_bucket") - F.col("g.avg_running_premium_bucket"), 2).alias("running_premium_bucket_minus_avg"),
    )
)

joined_holder = (
    above_average.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.premium_bucket",
        "s.days_start_date_to_end_date",
        "s.holder_id_sum_premium",
        "s.running_premium_bucket",
        "s.premium_rank",
        "s.premium_bucket_max_6_prior",
        "s.holder_id_max_premium_bucket",
        "s.holder_id_min_premium",
        "s.avg_running_premium_bucket",
        "s.running_premium_bucket_minus_avg",
        F.col("p.segment").alias("holder_segment"),
    )
)

result = (
    joined_holder
    .groupingSets(
        [["holder_id", "product"], ["holder_id"]],
        "holder_id", "product",
    )
    .agg(
        F.grouping("holder_id").alias("is_total_holder_id"),
        F.sum(F.when(F.col("holder_segment") == "VIP", F.col("premium_bucket")).otherwise(0)).alias("premium_bucket_holder_segment_vip"),
        F.sum("premium_rank").alias("total_premium_rank"),
        F.countDistinct("holder_id").alias("distinct_holder_ids"),
    )
    .orderBy(F.col("holder_id").asc_nulls_last(), F.col("product").asc_nulls_last())
)
