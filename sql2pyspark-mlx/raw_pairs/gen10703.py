from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k").orderBy("log_ts", "id", "k").rowsBetween(-6, -1)
w2 = Window.orderBy("status_code", "id", "k")
w3 = Window.partitionBy("level").orderBy("status_code_avg_6_prior", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("prev_status_code", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("prev_status_code", "id", "k")
w6 = Window.partitionBy(F.date_trunc("MONTH", "log_ts"), "k").orderBy(F.col("prev_status_code").desc(), "id", "k")
w7 = Window.partitionBy("level").orderBy(F.col("prev_status_code_bucket_2").desc(), "id", "k")

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "status_code",
        F.explode("context").alias("k", "attr_value"),
    )
)

with_windows = (
    exploded
    .select(
        "id",
        "log_ts",
        "level",
        "status_code",
        "k",
        "attr_value",
        F.avg("status_code").over(w).alias("status_code_avg_6_prior"),
        F.lag("status_code", 2).over(w2).alias("prev_status_code"),
    )
)

scored = (
    with_windows
    .filter(F.col("status_code_avg_6_prior").between(350, 358))
    .select(
        "id",
        "level",
        "k",
        "prev_status_code",
        F.sum("prev_status_code").over(w3).alias("cumulative_prev_status_code"),
        F.sum("status_code").over(w4).alias("running_status_code"),
        F.ntile(5).over(w5).alias("prev_status_code_bucket"),
        F.ntile(10).over(w6).alias("prev_status_code_bucket_2"),
    )
)

ranked = (
    scored
    .withColumn("row_num", F.row_number().over(w7))
    .filter(F.col("row_num") == 1)
)

daily = (
    ranked
    .groupBy(
        F.when(F.col("prev_status_code_bucket") >= 8, "HIGH").otherwise("LOW").alias("prev_status_code_bucket_tier"),
        "level",
    )
    .agg(
        F.sum(F.when(F.col("level") == "INFO", F.col("cumulative_prev_status_code")).otherwise(0)).alias("info_cumulative_prev_status_code"),
        F.round(F.sum("prev_status_code_bucket") / F.nullif(F.count(F.when(F.col("k") == "region", 1)), F.lit(0)), 2).alias("prev_status_code_bucket_per_region"),
        F.count_if(F.col("k") == "region").alias("region_events"),
    )
)

result = (
    daily
    .select(
        "prev_status_code_bucket_tier",
        "level",
        "info_cumulative_prev_status_code",
        "region_events",
    )
)
