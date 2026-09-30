from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("host", "k").orderBy("latency_ms", "id", "k")
w2 = Window.orderBy("next_has_fault", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("host", "k").orderBy("next_status_code", "id", "k")
w4 = Window.partitionBy("host").orderBy(F.col("seq_no").asc_nulls_last())
w5 = Window.orderBy("next_status_code")
w_part = Window.orderBy("seq_no", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("service")
w8 = Window.orderBy("next_status_code_cume_dist", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("k").orderBy("seq_no", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("k", "attr_value"),
    )
)

parsed = (
    exploded
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        "status_code",
        "k",
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("has_fault"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

metrics = (
    parsed
    .filter(F.col("status_code") != 332)
    .select(
        "id",
        "service",
        "host",
        "status_code",
        "k",
        "status_code_high",
        F.lead("has_fault").over(w_grp).alias("next_has_fault"),
        F.lag("has_fault").over(w_grp).alias("prev_has_fault"),
        F.lead("status_code").over(w_grp).alias("next_status_code"),
        F.lag("status_code").over(w_grp).alias("prev_status_code"),
        F.row_number().over(w_grp).alias("seq_no"),
    )
)

enriched = (
    metrics
    .withColumn("last_known_prev_has_fault", F.last("prev_has_fault", ignorenulls=True).over(w2))
    .withColumn("seq_no_2", F.row_number().over(w_running))
    .withColumn("seq_no_cume_dist", F.cume_dist().over(w4))
    .withColumn("next_status_code_cume_dist", F.cume_dist().over(w5))
    .withColumn("running_status_code_high", F.sum("status_code_high").over(w_part))
)

scored = (
    enriched
    .select(
        "*",
        F.avg("prev_status_code").over(w7).alias("service_avg_prev_status_code"),
        F.sum("status_code_high").over(w8).alias("cumulative_status_code_high"),
        F.sum("last_known_prev_has_fault").over(w9).alias("cumulative_last_known_prev_has_fault"),
    )
)

cleaned = (
    scored
    .filter(F.col("seq_no_2") < 6)
    .withColumn("next_status_code_cume_dist_int", F.col("next_status_code_cume_dist").cast("int"))
    .withColumn("next_status_code_cume_dist_int_2", F.col("next_status_code_cume_dist").cast("int"))
)

result = (
    cleaned
    .select(
        "id",
        "service",
        "status_code",
        "k",
        "status_code_high",
        "next_has_fault",
        "prev_has_fault",
        "next_status_code",
        "last_known_prev_has_fault",
    )
)
