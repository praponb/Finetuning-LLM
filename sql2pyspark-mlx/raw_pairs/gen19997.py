from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id")
w2 = Window.partitionBy("store_id")
w3 = Window.partitionBy(F.date_trunc("WEEK", "sale_date"), "store_id")
w4 = Window.partitionBy("product_id")
w5 = Window.orderBy(F.col("corr_revenue_units").desc_nulls_first())
w6 = Window.orderBy(F.col("units_share").asc_nulls_first())
w7 = Window.orderBy(F.col("corr_revenue_units").desc_nulls_last())
w8 = Window.orderBy(F.col("store_id_avg_units").desc_nulls_first())
w9 = Window.orderBy("units_share_cume_dist")
w10 = Window.orderBy(F.col("store_id_stddev_samp_units").asc_nulls_last())
w11 = Window.orderBy(F.col("units_share").desc())

remaining_keys = (
    df_store_sales
    .select("product_id", "store_id")
    .subtract(
        df_store_sales
        .filter(F.col("revenue").between(1300, 2300))
        .select("product_id", "store_id")
    )
)

verified = (
    df_store_sales
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")) & (F.col("s.store_id") == F.col("k.store_id")), "inner")
    .select("s.*")
)

metrics = (
    verified
    .select(
        "id",
        "revenue",
        F.round((F.col("units") / F.sum("units").over(w)), 4).alias("units_share"),
        F.stddev_samp("units").over(w2).alias("store_id_stddev_samp_units"),
        F.avg("units").over(w).alias("store_id_avg_units"),
        F.corr("revenue", "units").over(w3).alias("corr_revenue_units"),
        F.count("units").over(w4).alias("product_id_count_units"),
    )
)

with_windows = (
    metrics
    .withColumn("corr_revenue_units_dense_rank", F.dense_rank().over(w5))
    .withColumn("units_share_cume_dist", F.cume_dist().over(w6))
    .withColumn("corr_revenue_units_percent_rank", F.percent_rank().over(w7))
    .withColumn("store_id_avg_units_percent_rank", F.percent_rank().over(w8))
)

scored = (
    with_windows
    .select(
        "*",
        F.rank().over(w9).alias("units_share_cume_dist_rank"),
        F.rank().over(w9).alias("units_share_cume_dist_rank_2"),
        F.cume_dist().over(w10).alias("store_id_stddev_samp_units_cume_dist"),
        F.cume_dist().over(w11).alias("units_share_cume_dist_2"),
    )
)

result = (
    scored
    .withColumn("units_share_cume_dist_2_tier", F.when(F.col("units_share_cume_dist_2") >= 0.82, "LARGE").otherwise("SMALL"))
    .withColumn("corr_revenue_units_percent_rank_tier", F.when(F.col("corr_revenue_units_percent_rank") >= 0.78, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["units_share_cume_dist_2_tier", "corr_revenue_units_percent_rank_tier"], ["units_share_cume_dist_2_tier"]],
        "units_share_cume_dist_2_tier",
        "corr_revenue_units_percent_rank_tier",
    )
    .agg(
        F.percentile_approx("units_share", [0.25, 0.5, 0.75]).alias("units_share_quartiles"),
        F.max("store_id_avg_units_percent_rank").alias("max_store_id_avg_units_percent_rank"),
        F.grouping_id().alias("gid"),
    )
    .filter(F.col("max_store_id_avg_units_percent_rank") > 0)
    .orderBy("max_store_id_avg_units_percent_rank")
)
