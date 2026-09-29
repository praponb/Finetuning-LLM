from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier")
w2 = Window.partitionBy("carrier").orderBy(F.col("cost").desc_nulls_first())
w3 = Window.partitionBy("order_id", "status")
w4 = Window.partitionBy("status").orderBy(F.col("is_lost").desc_nulls_last())
w5 = Window.partitionBy("status")
w_running = Window.orderBy(F.col("status_stddev_samp_is_lost").desc_nulls_last())
w_running_2 = Window.partitionBy("order_id")

cleaned = (
    df_shipments
    .filter(F.col("carrier").isNotNull())
    .withColumn("is_lost", F.when(F.col("status") == "LOST", 1).otherwise(0))
    .withColumn("resolved_carrier", F.coalesce("carrier", F.lit("NONE")))
)

metrics = (
    cleaned
    .select(
        "*",
        F.stddev_samp("cost").over(w).alias("carrier_stddev_samp_cost"),
        F.dense_rank().over(w2).alias("cost_dense_rank"),
    )
)

remaining_keys = (
    metrics
    .select("id", "status")
    .subtract(
        df_shipments
        .filter(F.col("shipped_date") >= "2024-05-01")
        .select("id", "status")
    )
)

kept_rows = (
    metrics
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

scored = (
    kept_rows
    .select(
        "*",
        F.sum("carrier_stddev_samp_cost").over(w3).alias("order_id_sum_carrier_stddev_samp_cost"),
        F.cume_dist().over(w4).alias("is_lost_cume_dist"),
        F.stddev_samp("is_lost").over(w5).alias("status_stddev_samp_is_lost"),
        F.round((F.col("cost_dense_rank") / F.sum("cost_dense_rank").over(w5)), 4).alias("cost_dense_rank_share"),
    )
)

result = (
    scored
    .withColumn("status_stddev_samp_is_lost_rank", F.rank().over(w_running))
    .withColumn("order_id_min_cost_dense_rank", F.min("cost_dense_rank").over(w_running_2))
    .withColumn("cost_dense_rank_share_2", F.round((F.col("cost_dense_rank") / F.sum("cost_dense_rank").over(w5)), 4))
    .withColumn("status_stddev_samp_carrier_stddev_samp_cost", F.stddev_samp("carrier_stddev_samp_cost").over(w5))
)
