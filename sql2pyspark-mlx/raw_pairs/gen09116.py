from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "method").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("order_id").orderBy("running_sum_amount", "id").rowsBetween(-2, Window.currentRow)
w3 = Window.partitionBy("order_id").orderBy("running_sum_amount", "id").rowsBetween(-2, -1)
w4 = Window.partitionBy("order_id").orderBy("last3_min_amount")
w_grp = Window.orderBy("last3_sum_amount_avg_2_prior", "id").rowsBetween(-5, -1)
w6 = Window.orderBy("last3_sum_amount_avg_2_prior", "id").rowsBetween(-6, -1)
w7 = Window.orderBy("last3_sum_amount_avg_2_prior")
w8 = Window.orderBy("last3_sum_amount_avg_2_prior_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.orderBy("last3_min_amount_cume_dist", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.orderBy(F.col("last3_avg_amount_avg_6_prior").asc_nulls_last())
w_part = Window.orderBy("last_known_last3_min_amount_cume_dist", "id").rowsBetween(-6, -1)
w12 = Window.orderBy("last3_avg_amount_avg_6_prior", "id").rowsBetween(-6, Window.currentRow)

with_windows = (
    df_payments
    .filter(F.col("paid_at") < "2023-11-01")
    .select(
        "id",
        "order_id",
        "amount",
        F.sum("amount").over(w).alias("running_sum_amount"),
        F.count("amount").over(w).alias("running_count_amount"),
        F.avg("amount").over(w).alias("running_avg_amount"),
    )
)

scored = (
    with_windows
    .select(
        "id",
        "order_id",
        "running_sum_amount",
        F.avg("amount").over(w2).alias("last3_avg_amount"),
        F.min("amount").over(w2).alias("last3_min_amount"),
        F.sum("amount").over(w2).alias("last3_sum_amount"),
    )
)

scored_payments = (
    scored
    .select(
        "id",
        "running_sum_amount",
        "last3_avg_amount",
        "last3_sum_amount",
        F.avg("last3_sum_amount").over(w3).alias("last3_sum_amount_avg_2_prior"),
        F.cume_dist().over(w4).alias("last3_min_amount_cume_dist"),
    )
)

scored_payments_2 = (
    scored_payments
    .select(
        "id",
        "running_sum_amount",
        "last3_avg_amount",
        "last3_sum_amount_avg_2_prior",
        "last3_min_amount_cume_dist",
        F.avg("last3_min_amount_cume_dist").over(w_grp).alias("last3_min_amount_cume_dist_avg_5_prior"),
        F.avg("last3_avg_amount").over(w6).alias("last3_avg_amount_avg_6_prior"),
        F.rank().over(w7).alias("last3_sum_amount_avg_2_prior_rank"),
    )
)

valid = (
    scored_payments_2
    .filter(F.col("last3_sum_amount_avg_2_prior_rank") != 6)
    .withColumn("last3_avg_amount_avg_6_prior_num", F.col("last3_avg_amount_avg_6_prior").cast("double"))
)

with_windows_payments = (
    valid
    .select(
        "*",
        F.last("last3_min_amount_cume_dist", ignorenulls=True).over(w8).alias("last_known_last3_min_amount_cume_dist"),
        F.sum("running_sum_amount").over(w9).alias("running_running_sum_amount"),
        F.rank().over(w10).alias("last3_avg_amount_avg_6_prior_rank"),
    )
)

with_windows_payments_2 = (
    with_windows_payments
    .select(
        "id",
        "running_sum_amount",
        "last3_avg_amount",
        "last3_min_amount_cume_dist_avg_5_prior",
        "last_known_last3_min_amount_cume_dist",
        F.avg("running_sum_amount").over(w_part).alias("running_sum_amount_avg_6_prior"),
        F.avg("last3_avg_amount_avg_6_prior").over(w12).alias("last3_avg_amount_avg_6_prior_avg_6"),
    )
)

result = (
    with_windows_payments_2
    .groupBy(
        F.when(F.col("last3_min_amount_cume_dist_avg_5_prior") >= 1, "LARGE").otherwise("SMALL").alias("last3_min_amount_cume_dist_avg_5_prior_tier"),
    )
    .agg(
        F.round(F.avg("running_sum_amount"), 2).alias("avg_running_sum_amount"),
        F.sum("last3_avg_amount_avg_6_prior_avg_6").alias("total_last3_avg_amount_avg_6_prior_avg_6"),
        F.round(F.avg("last_known_last3_min_amount_cume_dist"), 2).alias("avg_last_known_last3_min_amount_cume_dist"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("avg_running_sum_amount") >= 0)
    .orderBy(F.col("total_last3_avg_amount_avg_6_prior_avg_6").desc())
)
