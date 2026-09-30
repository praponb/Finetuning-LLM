from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_id = Window.partitionBy("holder_id")
w_premium_tier_holder_id_by_en = Window.partitionBy("premium_tier", "holder_id").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_premium_tier_holder_id_by_en_2 = Window.partitionBy("premium_tier", "holder_id").orderBy("end_date", "id")
w_premium_tier_status_by_start = Window.partitionBy("premium_tier", "status").orderBy("start_date")
w_all_by_distinct_premium_tier = Window.orderBy(F.col("distinct_premium_tier_in_holder_id").desc(), "id").rowsBetween(-6, Window.currentRow)
w_premium_tier_start_date_rank = Window.partitionBy("premium_tier", "start_date_rank").orderBy("distinct_premium_tier_in_holder_id", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_distinct_premium_tier_2 = Window.orderBy(F.col("distinct_premium_tier_in_holder_id").desc(), "id")
w_all_by_distinct_premium_tier_3 = Window.orderBy(F.col("distinct_premium_tier_in_holder_id").desc(), "id").rowsBetween(-2, Window.currentRow)
w_premium_tier_start_date_rank_2 = Window.partitionBy("premium_tier", "start_date_rank").orderBy("distinct_premium_tier_in_holder_id", "id")
w_premium_tier_start_date_rank_3 = Window.partitionBy("premium_tier", "start_date_rank").orderBy("distinct_premium_tier_in_holder_id")

cleaned_policys = (
    df_policies
    .filter(F.col("holder_id") <= 18)
    .select(
        "id",
        "holder_id",
        "start_date",
        "end_date",
        "status",
        F.round(F.col("premium") * 1.1, 2).alias("premium_adj"),
        F.when(F.col("premium") >= 3719.7, "HIGH").when(F.col("premium") >= 2111.3, "MEDIUM").otherwise("LOW").alias("premium_tier"),
    )
)

windowed = (
    cleaned_policys
    .select(
        "*",
        F.round(F.col("premium_adj") - F.avg("premium_adj").over(w_holder_id), 2).alias("premium_adj_vs_holder_id_avg"),
        F.size(F.collect_set("status").over(w_holder_id)).alias("distinct_status_in_holder_id"),
        F.sum("premium_adj").over(w_holder_id).alias("holder_id_sum_premium_adj"),
        F.count(F.lit(1)).over(w_holder_id).alias("rows_in_holder_id"),
        F.sum("premium_adj").over(w_premium_tier_holder_id_by_en).alias("running_premium_adj"),
        F.round(F.col("premium_adj") / F.nullif(F.sum("premium_adj").over(w_holder_id), F.lit(0)), 4).alias("premium_adj_share_of_holder_id"),
        F.size(F.collect_set("premium_tier").over(w_holder_id)).alias("distinct_premium_tier_in_holder_id"),
        F.round(F.cume_dist().over(w_premium_tier_holder_id_by_en_2), 3).alias("end_date_cume_dist"),
        F.max("premium_adj").over(w_holder_id).alias("holder_id_max_premium_adj"),
        F.rank().over(w_premium_tier_status_by_start).alias("start_date_rank"),
    )
)

tagged = (
    windowed
    .filter(F.col("holder_id").isNotNull())
    .select(
        "*",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("start_date")).alias("days_since_start_date"),
        F.lower(F.regexp_replace(F.trim(F.col("status")), " ", "_")).alias("status_slug"),
        F.round(F.col("running_premium_adj") * 1.5, 2).alias("running_premium_adj_adj"),
    )
)

windowed_2 = (
    tagged
    .select(
        "*",
        F.round(F.avg("premium_adj").over(w_all_by_distinct_premium_tier), 2).alias("premium_adj_avg_7"),
        F.nth_value("premium_adj_vs_holder_id_avg", 2).over(w_premium_tier_start_date_rank).alias("second_premium_adj_vs_holder_id_avg"),
        F.rank().over(w_all_by_distinct_premium_tier_2).alias("distinct_premium_tier_in_holder_id_rank"),
        F.max("rows_in_holder_id").over(w_all_by_distinct_premium_tier_3).alias("rows_in_holder_id_max_3"),
        (F.col("running_premium_adj_adj") - F.lag("running_premium_adj_adj").over(w_premium_tier_start_date_rank_2)).alias("running_premium_adj_adj_change"),
        F.dense_rank().over(w_premium_tier_start_date_rank_3).alias("distinct_premium_tier_in_holder_id_dense_rank"),
        F.round(F.avg("end_date_cume_dist").over(w_all_by_distinct_premium_tier), 2).alias("end_date_cume_dist_avg_7"),
    )
)

result = (
    windowed_2
    .select(
        "id",
        "holder_id",
        "start_date",
        "status",
        "premium_adj",
        "premium_tier",
        "premium_adj_vs_holder_id_avg",
        "running_premium_adj",
        "premium_adj_share_of_holder_id",
        "end_date_cume_dist",
        "holder_id_max_premium_adj",
        "start_date_rank",
        "running_premium_adj_adj",
        "rows_in_holder_id_max_3",
        "distinct_premium_tier_in_holder_id_dense_rank",
        "end_date_cume_dist_avg_7",
    )
    .orderBy(
        F.col("start_date_rank").desc(),
        F.col("running_premium_adj_adj").desc(),
        "id",
    )
    .limit(10)
)
