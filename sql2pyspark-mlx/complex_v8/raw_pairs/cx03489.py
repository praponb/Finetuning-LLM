from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_min_status_len_by_stddev_sta = Window.partitionBy("min_status_len").orderBy(F.col("stddev_status_len").desc(), "start_date_ym", "premium_tier")
w_min_status_len_by_stddev_sta_2 = Window.partitionBy("min_status_len").orderBy(F.col("stddev_status_len").desc(), "start_date_ym", "premium_tier").rowsBetween(-1, Window.currentRow)
w_premium_tier_min_status_len = Window.partitionBy("premium_tier", "min_status_len").orderBy("min_status_len", "start_date_ym", "premium_tier").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_premium_tier_min_status_len_2 = Window.partitionBy("premium_tier", "min_status_len").orderBy("min_status_len")
w_premium_tier = Window.partitionBy("premium_tier")
w_start_date_ym_len_premium_ti = Window.partitionBy("start_date_ym_len", "premium_tier").orderBy(F.col("min_status_len_dense_rank").desc(), "start_date_ym", "premium_tier")
w_start_date_ym_len_premium_ti_2 = Window.partitionBy("start_date_ym_len", "premium_tier").orderBy(F.col("min_status_len_dense_rank").desc())
w_min_status_len_dense_rank = Window.partitionBy("min_status_len_dense_rank")
w_start_date_ym_len_by_min_sta = Window.partitionBy("start_date_ym_len").orderBy(F.col("min_status_len_max_2").desc(), "start_date_ym", "premium_tier")

base = (
    df_policies
    .filter(F.col("premium") >= 1051.9)
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "end_date",
        "status",
        F.length(F.trim(F.col("status"))).alias("status_len"),
        F.substring(F.trim(F.col("product")), 1, 4).alias("product_prefix"),
        F.when(F.col("premium") >= 3544.1, "A").when(F.col("premium") >= 2033.8, "B").otherwise("C").alias("premium_tier"),
    )
)

per_group = (
    base
    .filter(F.col("product_prefix").isNotNull())
    .groupBy(
        F.date_format("start_date", "yyyy-MM").alias("start_date_ym"),
        "premium_tier",
    )
    .agg(
        F.array_sort(F.collect_set("product")).alias("product_list"),
        F.round(F.stddev("status_len"), 2).alias("stddev_status_len"),
        F.min("status_len").alias("min_status_len"),
    )
)

windowed = (
    per_group
    .select(
        "*",
        F.lead("stddev_status_len", 1, 0).over(w_min_status_len_by_stddev_sta).alias("next_stddev_status_len"),
        F.max("min_status_len").over(w_min_status_len_by_stddev_sta_2).alias("min_status_len_max_2"),
        F.last("stddev_status_len", ignorenulls=True).over(w_premium_tier_min_status_len).alias("last_known_stddev_status_len"),
        F.dense_rank().over(w_premium_tier_min_status_len_2).alias("min_status_len_dense_rank"),
        F.count(F.lit(1)).over(w_premium_tier).alias("rows_in_premium_tier"),
        F.round(F.col("min_status_len") - F.avg("min_status_len").over(w_premium_tier), 2).alias("min_status_len_vs_premium_tier_avg"),
    )
)

scored = (
    windowed
    .select(
        "*",
        F.length(F.trim(F.col("start_date_ym"))).alias("start_date_ym_len"),
    )
)

sequenced = (
    scored
    .select(
        "*",
        F.lead("min_status_len").over(w_start_date_ym_len_premium_ti).alias("next_min_status_len"),
        F.dense_rank().over(w_start_date_ym_len_premium_ti_2).alias("min_status_len_dense_rank_dense_rank"),
        F.round(F.col("min_status_len_dense_rank") - F.avg("min_status_len_dense_rank").over(w_min_status_len_dense_rank), 2).alias("min_status_len_dense_rank_vs_min_status_len_dens"),
        F.row_number().over(w_start_date_ym_len_by_min_sta).alias("rn"),
        F.round(F.cume_dist().over(w_start_date_ym_len_premium_ti_2), 3).alias("min_status_len_dense_rank_cume_dist"),
        F.sum("next_stddev_status_len").over(w_min_status_len_dense_rank).alias("min_status_len_dense_rank_sum_next_stddev_status"),
    )
)

derived = (
    sequenced
    .select(
        "start_date_ym",
        "premium_tier",
        "product_list",
        "min_status_len",
        "next_stddev_status_len",
        "min_status_len_max_2",
        "rows_in_premium_tier",
        "min_status_len_vs_premium_tier_avg",
        "start_date_ym_len",
        "min_status_len_dense_rank_dense_rank",
        "rn",
        "min_status_len_dense_rank_cume_dist",
        "min_status_len_dense_rank_sum_next_stddev_status",
        (F.col("min_status_len_max_2") + F.col("stddev_status_len")).alias("min_status_len_max_2_plus_stddev_status_len"),
        F.round(F.col("stddev_status_len") * 0.25, 2).alias("stddev_status_len_adj"),
    )
)

result = (
    derived
    .select(
        "start_date_ym",
        "premium_tier",
        F.col("min_status_len_vs_premium_tier_avg").alias("min_status_len_vs_premium_tier_avg_v"),
        F.col("min_status_len_max_2_plus_stddev_status_len").alias("m_min_status_len_max_2_plus_stddev_status_len"),
    )
    .unpivot(
        ids=["start_date_ym", "premium_tier"],
        values=["min_status_len_vs_premium_tier_avg_v", "m_min_status_len_max_2_plus_stddev_status_len"],
        variableColumnName="metric_name",
        valueColumnName="amount",
    )
    .orderBy("start_date_ym", "premium_tier", "metric_name")
)
