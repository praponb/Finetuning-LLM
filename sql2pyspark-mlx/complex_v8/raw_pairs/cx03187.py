from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_id_by_premium_prior_2 = Window.partitionBy("holder_id").orderBy(F.col("premium").desc(), "id").rowsBetween(-2, -1)
w_holder_id_by_premium = Window.partitionBy("holder_id").orderBy("premium", "id")
w_product_group_status_by_prem = Window.partitionBy("product_group", "status").orderBy("premium", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_status = Window.partitionBy("status")
w_product_group_status_by_prem_2 = Window.partitionBy("product_group", "status").orderBy("premium")
w_product_group_product = Window.partitionBy("product_group", "product")
w_all_by_premium = Window.orderBy(F.col("premium").desc(), "id")
w_premium_rank_2_by_premium_av = Window.partitionBy("premium_rank_2").orderBy(F.col("premium_avg_2_prior").desc())
w_all_by_running_count = Window.orderBy("running_count", "id")
w_all_by_running_count_running = Window.orderBy("running_count", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared = (
    df_policies
    .filter((F.col("status") == "CANCELLED") & (F.col("start_date") >= F.lit("2024-11-20").cast("date")))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "status",
        F.when(F.col("product").isin("HEALTH", "AUTO"), "group_a").when(F.col("product") == "LIFE", "group_b").otherwise("other").alias("product_group"),
    )
)

metrics = (
    prepared
    .select(
        "*",
        F.round(F.avg("premium").over(w_holder_id_by_premium_prior_2), 2).alias("premium_avg_2_prior"),
        F.rank().over(w_holder_id_by_premium).alias("premium_rank"),
        F.count(F.lit(1)).over(w_product_group_status_by_prem).alias("running_count"),
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_status), F.lit(0)), 4).alias("premium_share_of_status"),
        F.rank().over(w_product_group_status_by_prem_2).alias("premium_rank_2"),
        F.size(F.collect_set("product_group").over(w_status)).alias("distinct_product_group_in_status"),
        F.round(F.avg("premium").over(w_product_group_product), 2).alias("product_group_product_avg_premium"),
        F.rank().over(w_all_by_premium).alias("premium_rank_3"),
    )
)

metrics_2 = (
    metrics
    .select(
        "*",
        F.round(F.percent_rank().over(w_premium_rank_2_by_premium_av), 3).alias("premium_avg_2_prior_percent_rank"),
        F.min("premium_rank").over(w_status).alias("status_min_premium_rank"),
        F.round(F.cume_dist().over(w_all_by_running_count), 3).alias("running_count_cume_dist"),
        F.sum("premium_rank_3").over(w_all_by_running_count_running).alias("running_premium_rank_3"),
        F.lag("premium", 2, 0).over(w_all_by_running_count).alias("prev2_premium"),
    )
)

result = (
    metrics_2
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "status",
        "premium_avg_2_prior",
        "premium_rank",
        "premium_share_of_status",
        "premium_rank_2",
        "distinct_product_group_in_status",
        "product_group_product_avg_premium",
        "premium_rank_3",
        "premium_avg_2_prior_percent_rank",
        "status_min_premium_rank",
        "running_count_cume_dist",
        "running_premium_rank_3",
        "prev2_premium",
        F.least(F.col("premium_rank_2"), F.col("premium_rank")).alias("least_premium_rank_2_premium_rank"),
    )
    .orderBy(F.col("status_min_premium_rank").desc(), F.col("premium_rank_3").asc())
)
