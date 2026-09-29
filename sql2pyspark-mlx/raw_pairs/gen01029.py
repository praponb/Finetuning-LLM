from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("opened_date"), "customer_id").orderBy("balance", "id").rowsBetween(-5, Window.currentRow)
w2 = Window.partitionBy("customer_id").orderBy("balance", "id").rowsBetween(-5, -1)
w3 = Window.partitionBy("status").orderBy("balance", "id")
w4 = Window.partitionBy("status").orderBy("balance_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy(F.col("running_sum_balance_avg_5").desc_nulls_first())
w6 = Window.partitionBy("customer_id").orderBy(F.col("running_count_balance_avg_5").desc_nulls_first())
w7 = Window.partitionBy("status")
w_grp = Window.partitionBy("customer_id", "status")

enriched = (
    df_accounts
    .filter(F.year("opened_date") == 2023)
    .select(
        "*",
        F.avg("balance").over(w).alias("balance_avg_5"),
        F.avg("balance").over(w2).alias("balance_avg_5_prior"),
        F.ntile(4).over(w3).alias("balance_bucket"),
    )
)

metrics = (
    enriched
    .select(
        "id",
        "customer_id",
        "balance",
        "status",
        "balance_avg_5_prior",
        "balance_bucket",
        F.min("balance_avg_5").over(w4).alias("running_min_balance_avg_5"),
        F.count("balance_avg_5").over(w4).alias("running_count_balance_avg_5"),
        F.sum("balance_avg_5").over(w4).alias("running_sum_balance_avg_5"),
        F.max("balance_avg_5").over(w4).alias("running_max_balance_avg_5"),
    )
)

matched_keys = (
    metrics
    .select("customer_id", "id")
    .intersect(
        df_accounts
        .filter(F.col("account_type") == "LOAN")
        .select("customer_id", "id")
    )
)

verified = (
    metrics
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

metrics_accounts = (
    verified
    .withColumn("running_sum_balance_avg_5_dense_rank", F.dense_rank().over(w5))
    .withColumn("running_count_balance_avg_5_cume_dist", F.cume_dist().over(w6))
    .withColumn("status_min_balance_bucket", F.min("balance_bucket").over(w7))
    .withColumn("customer_id_count_running_sum_balance_avg_5", F.count("running_sum_balance_avg_5").over(w_grp))
    .withColumn("customer_id_stddev_samp_running_count_balance_avg_5", F.stddev_samp("running_count_balance_avg_5").over(w_grp))
)

eligible = (
    metrics_accounts
    .alias("s")
    .join(df_transactions.alias("x"), (F.col("x.account_id") == F.col("s.id")) & (F.col("x.account_id") == 34), "left_anti")
)

result = (
    eligible
    .groupBy("status")
    .agg(
        F.countDistinct("status").alias("distinct_status"),
        F.sum("status_min_balance_bucket").alias("total_status_min_balance_bucket"),
        F.round(F.sum("running_max_balance_avg_5") / F.nullif(F.count(F.when(F.col("status") == "FROZEN", 1)), F.lit(0)), 2).alias("running_max_balance_avg_5_per_frozen"),
        F.size(F.collect_set("status")).alias("num_status"),
        F.round(F.avg("balance_avg_5_prior"), 2).alias("avg_balance_avg_5_prior"),
        F.countDistinct("status").alias("distinct_status_2"),
    )
    .orderBy(F.col("running_max_balance_avg_5_per_frozen").desc())
)
