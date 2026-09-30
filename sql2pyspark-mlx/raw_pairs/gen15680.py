from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("key").orderBy("status_code_high", "id", "key")
w_recent = Window.orderBy(F.col("status_code").desc())
w_running = Window.partitionBy("key").orderBy("prev_status_code_high", "id", "key").rowsBetween(-5, -1)
w_recent_2 = Window.orderBy("status_code", "id", "key").rowsBetween(-6, Window.currentRow)

lines = (
    df_app_logs
    .select(
        "id",
        "host",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("key", "attr_value"),
    )
)

unpacked = (
    lines
    .filter(F.col("status_code") <= 352)
    .select(
        "id",
        "status_code",
        "key",
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("flag_bit1"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

enriched = (
    unpacked
    .withColumn("next_flag_bit1", F.lead("flag_bit1").over(w))
    .withColumn("prev_flag_bit1", F.lag("flag_bit1").over(w))
    .withColumn("next_status_code_high", F.lead("status_code_high").over(w))
    .withColumn("prev_status_code_high", F.lag("status_code_high").over(w))
    .withColumn("row_seq", F.row_number().over(w))
)

cleaned = (
    enriched
    .filter(F.col("key").isNotNull() & ~F.col("key").isin(["trace", "version"]))
    .select(
        "id",
        "status_code",
        "key",
        "flag_bit1",
        "status_code_high",
        "next_flag_bit1",
        "prev_flag_bit1",
        "next_status_code_high",
        "prev_status_code_high",
        "row_seq",
        F.coalesce("status_code_high", F.lit(0)).alias("status_code_high_filled"),
        F.coalesce("key", F.lit("OTHER")).alias("resolved_key"),
    )
)

enriched_app_logs = (
    cleaned
    .withColumn("status_code_cume_dist", F.cume_dist().over(w_recent))
    .withColumn("status_code_high_avg_5_prior", F.avg("status_code_high").over(w_running))
    .withColumn("next_flag_bit1_avg_6", F.avg("next_flag_bit1").over(w_recent_2))
)

result = (
    enriched_app_logs
    .select(
        "id",
        "key",
        F.col("status_code_high_filled").alias("STATUS_CODE_HIGH_FILLED_VAL"),
        F.col("status_code_cume_dist").alias("STATUS_CODE_CUME_DIST_VAL"),
    )
    .unpivot(
        ids=["id", "key"],
        values=["STATUS_CODE_HIGH_FILLED_VAL", "STATUS_CODE_CUME_DIST_VAL"],
        variableColumnName="kpi",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
