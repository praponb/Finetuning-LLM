from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key")
w2 = Window.partitionBy("attr_key").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w_running = Window.partitionBy("attr_key").orderBy(F.col("status_code").desc())
w4 = Window.partitionBy("attr_key").orderBy("status_code")
w5 = Window.partitionBy("attr_key").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w6 = Window.orderBy(F.col("status_code_avg_1h").asc_nulls_last())
w7 = Window.partitionBy(F.month("log_ts")).orderBy(F.col("status_code_dense_rank").desc_nulls_last())
w8 = Window.orderBy("status_code_dense_rank")
w9 = Window.partitionBy(F.month("log_ts"), "attr_key")
w_ordered = Window.orderBy(F.col("status_code").desc())
w11 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w12 = Window.orderBy(F.col("status_code_dense_rank_dense_rank").desc_nulls_last())
w13 = Window.orderBy(F.col("log_ts_month_max_status_code_percent_rank").desc_nulls_last())
w14 = Window.partitionBy(F.month("log_ts"))

exploded = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "status_code",
        F.explode_outer("context").alias("attr_key", "value"),
    )
)

kept_keys = (
    exploded
    .select("id")
    .intersectAll(
        df_app_logs
        .filter(F.col("log_ts") < "2023-07-01")
        .select("id")
    )
)

kept_rows = (
    exploded
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched = (
    kept_rows
    .select(
        "*",
        F.min("status_code").over(w).alias("attr_key_min_status_code"),
        F.avg("status_code").over(w2).alias("status_code_avg_1h"),
        F.percent_rank().over(w_running).alias("status_code_percent_rank"),
        F.cume_dist().over(w4).alias("status_code_cume_dist"),
        F.dense_rank().over(w_running).alias("status_code_dense_rank"),
    )
)

metrics = (
    enriched
    .filter(F.col("attr_key_min_status_code") == 424)
    .withColumn("status_code_avg_1h_count_1h", F.count("status_code_avg_1h").over(w2))
    .withColumn("status_code_avg_1h_sum_2h", F.sum("status_code_avg_1h").over(w5))
)

enriched_app_logs = (
    metrics
    .select(
        "log_ts",
        "status_code",
        "value",
        "status_code_percent_rank",
        "status_code_avg_1h_count_1h",
        "status_code_avg_1h_sum_2h",
        F.cume_dist().over(w6).alias("status_code_avg_1h_cume_dist"),
        F.dense_rank().over(w7).alias("status_code_dense_rank_dense_rank"),
        F.rank().over(w8).alias("status_code_dense_rank_rank"),
        F.max("status_code_percent_rank").over(w9).alias("log_ts_month_max_status_code_percent_rank"),
    )
)

metrics_app_logs = (
    enriched_app_logs
    .select(
        "*",
        F.rank().over(w_ordered).alias("status_code_rank"),
        F.max("status_code").over(w11).alias("status_code_max_2h"),
        F.percent_rank().over(w12).alias("status_code_dense_rank_dense_rank_percent_rank"),
        F.percent_rank().over(w13).alias("log_ts_month_max_status_code_percent_rank_percent_rank"),
        F.round((F.col("status_code") / F.sum("status_code").over(w14)), 4).alias("status_code_share"),
    )
)

result = (
    metrics_app_logs
    .groupBy(
        F.when(F.col("status_code_dense_rank_dense_rank_percent_rank") >= 0.62, "PREMIUM").otherwise("STANDARD").alias("status_code_dense_rank_dense_rank_percent_rank_tier"),
    )
    .agg(
        ((F.unix_timestamp(F.max("log_ts")) - F.unix_timestamp(F.min("log_ts"))) / 60).alias("duration_min"),
        F.bit_and("status_code").alias("status_code_bit_and"),
        F.avg("status_code_dense_rank_dense_rank_percent_rank").alias("avg_status_code_dense_rank_dense_rank_percent_rank"),
    )
)
