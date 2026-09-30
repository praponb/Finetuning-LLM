from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("balance", "id")
w2 = Window.partitionBy("status", "customer_id").orderBy("rank_in_group", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status").orderBy(F.col("running_sum_balance").desc_nulls_first(), "id")
w4 = Window.partitionBy("customer_id", "status").orderBy(F.col("running_min_balance").desc(), "id")

deduped = (
    df_accounts
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

with_windows = (
    deduped
    .select(
        "id",
        "customer_id",
        "balance",
        "status",
        "rank_in_group",
        F.sum("balance").over(w2).alias("running_sum_balance"),
        F.max("balance").over(w2).alias("running_max_balance"),
        F.min("balance").over(w2).alias("running_min_balance"),
    )
)

latest = (
    with_windows
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
)

deduped_accounts = (
    latest
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select("id", "rank_in_group", "running_max_balance", "row_num")
)

result = (
    deduped_accounts
    .groupBy(
        F.when(F.col("running_max_balance") >= 65000, "PREMIUM").otherwise("STANDARD").alias("running_max_balance_tier"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.round(F.avg("running_max_balance"), 2).alias("avg_running_max_balance"),
        F.percentile_approx("running_max_balance", [0.25, 0.5, 0.75]).alias("running_max_balance_quartiles"),
        F.percentile_approx("running_max_balance", 0.5).alias("running_max_balance_p50"),
    )
    .filter(F.col("running_max_balance_p50") > 0)
    .orderBy(F.col("avg_running_max_balance").asc_nulls_last())
)
