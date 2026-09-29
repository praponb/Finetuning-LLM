from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service").orderBy(F.col("status_code").desc())
w2 = Window.partitionBy("host").orderBy("status_code", "id", "k").rowsBetween(-6, -1)
w3 = Window.orderBy("status_code", "id", "k")
w4 = Window.partitionBy("k").orderBy(F.col("status_code_avg_6_prior").asc_nulls_first(), "id", "k")
w5 = Window.partitionBy("service").orderBy(F.col("status_code_dense_rank").desc(), "id", "k")
w_grp = Window.orderBy("status_code_bucket", "id", "k")
w_part = Window.partitionBy("host").orderBy(F.col("status_code_dense_rank").asc_nulls_first())
w_recent = Window.partitionBy("level").orderBy("status_code", "id", "k").rowsBetween(-6, Window.currentRow)
w9 = Window.partitionBy("k").orderBy("status_code", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("service").orderBy("status_code_dense_rank_avg_6", "id", "k").rowsBetween(-6, -1)
w11 = Window.orderBy(F.col("status_code_dense_rank").asc_nulls_first())
w12 = Window.partitionBy("k").orderBy(F.col("status_code_dense_rank_avg_6").desc())
w13 = Window.partitionBy("service")
w14 = Window.partitionBy("host").orderBy(F.col("status_code_dense_rank_avg_6").desc_nulls_first())
w15 = Window.partitionBy("k").orderBy("status_code_dense_rank_avg_6", "id", "k")
w16 = Window.partitionBy("status_code_bucket_tier").orderBy(F.col("h4_status_code_bucket_bucket").desc())

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "status_code",
        F.explode("context").alias("k", "attr_value"),
    )
)

metrics = (
    exploded
    .filter(F.col("status_code") == 267)
    .withColumn("status_code_dense_rank", F.dense_rank().over(w))
    .withColumn("status_code_avg_6_prior", F.avg("status_code").over(w2))
    .withColumn("status_code_bucket", F.ntile(4).over(w3))
)

deduped = (
    metrics
    .withColumn("row_num", F.row_number().over(w4))
    .filter(F.col("row_num") == 1)
)

deduped_app_logs = (
    deduped
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select(
        "id",
        "service",
        "host",
        "level",
        "status_code",
        "k",
        "status_code_dense_rank",
        "status_code_avg_6_prior",
        "status_code_bucket",
    )
)

metrics_app_logs = (
    deduped_app_logs
    .select(
        "*",
        F.ntile(5).over(w_grp).alias("status_code_bucket_bucket"),
        F.percent_rank().over(w_part).alias("status_code_dense_rank_percent_rank"),
        F.avg("status_code_dense_rank").over(w_recent).alias("status_code_dense_rank_avg_6"),
        F.sum("status_code_avg_6_prior").over(w9).alias("cumulative_status_code_avg_6_prior"),
    )
)

enriched = (
    metrics_app_logs
    .select(
        "*",
        F.avg("status_code_bucket_bucket").over(w10).alias("status_code_bucket_bucket_avg_6_prior"),
        F.dense_rank().over(w11).alias("status_code_dense_rank_dense_rank"),
    )
)

metrics_app_logs_2 = (
    enriched
    .select(
        "*",
        F.percent_rank().over(w12).alias("status_code_dense_rank_avg_6_percent_rank"),
        F.round((F.col("cumulative_status_code_avg_6_prior") / F.sum("cumulative_status_code_avg_6_prior").over(w13)), 4).alias("cumulative_status_code_avg_6_prior_share"),
        F.avg("status_code_dense_rank").over(w13).alias("service_avg_status_code_dense_rank"),
        F.percent_rank().over(w14).alias("status_code_dense_rank_avg_6_percent_rank_2"),
        F.lag("status_code_dense_rank_percent_rank", 1).over(w15).alias("prev_status_code_dense_rank_percent_rank"),
    )
)

daily = (
    metrics_app_logs_2
    .groupBy(
        F.when(F.col("status_code_bucket") >= 9, "LARGE").otherwise("SMALL").alias("status_code_bucket_tier"),
        "k",
        "service",
    )
    .agg(
        F.count_if(F.col("level") == "WARN").alias("warn_events"),
        F.sum(F.when(F.col("host") == "h4", F.col("status_code_bucket_bucket")).otherwise(0)).alias("h4_status_code_bucket_bucket"),
        F.max(F.when(F.col("host") == "h2", F.col("status_code"))).alias("h2_max_status_code"),
        F.count(F.when(F.col("level") == "INFO", 1)).alias("info_rows"),
    )
    .withColumn("h4_status_code_bucket_bucket_dense_rank", F.dense_rank().over(w16))
)

result = (
    daily
    .select("status_code_bucket_tier", "k", "service", "h4_status_code_bucket_bucket")
    .orderBy(F.col("h4_status_code_bucket_bucket").desc_nulls_first())
)
