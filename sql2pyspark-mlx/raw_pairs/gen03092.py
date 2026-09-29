from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host").orderBy("status_code", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("k").orderBy(F.col("status_code").desc(), "id", "k")
w3 = Window.partitionBy("level").orderBy("status_code", "id", "k").rowsBetween(-3, Window.currentRow)
w4 = Window.partitionBy("service").orderBy("dedup_rank", "id", "k").rowsBetween(-3, -1)
w5 = Window.partitionBy("k").orderBy("last4_max_status_code_bucket", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("service").orderBy(F.col("status_code_bucket").desc(), "id", "k")
w7 = Window.partitionBy("service").orderBy("last4_min_status_code_bucket", "id", "k")
w_grp = Window.partitionBy("level", "host").orderBy("running_last4_sum_status_code_bucket", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.orderBy("status_code_bucket_avg_3_prior", "id", "k").rowsBetween(-5, Window.currentRow)
w10 = Window.orderBy(F.col("running_min_status_code_bucket_bucket").desc_nulls_first())
w11 = Window.partitionBy("level").orderBy("status_code", "id", "k")

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "status_code",
        F.explode("context").alias("k", "value"),
    )
)

metrics = (
    flattened
    .filter(F.col("host").isin(["h3", "h4", "h1"]))
    .withColumn("first_known_k", F.first("k", ignorenulls=True).over(w))
    .withColumn("status_code_bucket", F.ntile(4).over(w2))
)

ranked = (
    metrics
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
)

metrics_app_logs = (
    ranked
    .withColumn("last4_min_status_code_bucket", F.min("status_code_bucket").over(w3))
    .withColumn("last4_max_status_code_bucket", F.max("status_code_bucket").over(w3))
    .withColumn("last4_sum_status_code_bucket", F.sum("status_code_bucket").over(w3))
    .withColumn("status_code_bucket_avg_3_prior", F.avg("status_code_bucket").over(w4))
)

metrics_app_logs_2 = (
    metrics_app_logs
    .select(
        "*",
        F.sum("last4_sum_status_code_bucket").over(w5).alias("running_last4_sum_status_code_bucket"),
        F.ntile(5).over(w6).alias("status_code_bucket_bucket"),
        F.lag("last4_sum_status_code_bucket", 1).over(w7).alias("prev_last4_sum_status_code_bucket"),
    )
)

scored = (
    metrics_app_logs_2
    .select(
        "*",
        F.min("status_code_bucket_bucket").over(w_grp).alias("running_min_status_code_bucket_bucket"),
        F.count("status_code_bucket_bucket").over(w_grp).alias("running_count_status_code_bucket_bucket"),
        F.avg("status_code_bucket_bucket").over(w_grp).alias("running_avg_status_code_bucket_bucket"),
        F.avg("prev_last4_sum_status_code_bucket").over(w_hist).alias("prev_last4_sum_status_code_bucket_avg_5"),
    )
)

metrics_app_logs_3 = (
    scored
    .select(
        "id",
        "service",
        "host",
        "level",
        "k",
        "last4_min_status_code_bucket",
        "last4_sum_status_code_bucket",
        "prev_last4_sum_status_code_bucket",
        "running_avg_status_code_bucket_bucket",
        F.percent_rank().over(w10).alias("running_min_status_code_bucket_bucket_percent_rank"),
        F.row_number().over(w11).alias("row_seq"),
    )
)

result = (
    metrics_app_logs_3
    .groupBy("level")
    .agg(
        F.count("*").alias("cnt"),
        F.count("*").alias("cnt_2"),
        F.count(F.when(F.col("service") == "billing", 1)).alias("billing_count"),
    )
)
