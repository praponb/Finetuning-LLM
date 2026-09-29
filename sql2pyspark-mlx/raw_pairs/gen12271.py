from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service").orderBy("flag_bit1", "id", "key").rowsBetween(-3, -1)
w2 = Window.partitionBy("service").orderBy("status_code", "id", "key")
w3 = Window.partitionBy("key").orderBy(F.col("status_code_high").desc(), "id", "key")
w4 = Window.partitionBy("key", "service")
w5 = Window.partitionBy("service").orderBy("flag_bit1", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy(F.col("key_min_status_code_high").desc(), "id", "key")
w7 = Window.partitionBy("key").orderBy("status_code_high", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "latency_ms",
        "status_code",
        F.explode("context").alias("key", "attr_value"),
    )
)

parsed = (
    flattened
    .filter(F.col("key") == "trace")
    .select(
        "id",
        "service",
        "status_code",
        "key",
        "attr_value",
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("flag_bit1"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

cleaned = (
    parsed
    .filter(F.col("key").isNotNull())
    .withColumn("status_code_band", F.when(F.col("status_code") >= 411, "High").when(F.col("status_code") >= 285, "Medium").otherwise("Low"))
    .withColumn("key_length", F.length("key"))
)

with_windows = (
    cleaned
    .select(
        "id",
        "key",
        "status_code_high",
        F.avg("flag_bit1").over(w).alias("flag_bit1_avg_3_prior"),
        F.row_number().over(w2).alias("row_seq"),
        F.row_number().over(w3).alias("row_seq_2"),
        F.min("status_code_high").over(w4).alias("key_min_status_code_high"),
        F.sum("status_code_high").over(w5).alias("running_status_code_high"),
    )
)

enriched = (
    with_windows
    .filter(F.col("status_code_high") >= 2)
    .select(
        "id",
        "key",
        "row_seq",
        "key_min_status_code_high",
        "running_status_code_high",
        F.ntile(10).over(w6).alias("key_min_status_code_high_bucket"),
        F.first("flag_bit1_avg_3_prior", ignorenulls=True).over(w7).alias("first_known_flag_bit1_avg_3_prior"),
    )
)

result = (
    enriched
    .select(
        "id",
        "key",
        "row_seq",
        "key_min_status_code_high",
        "key_min_status_code_high_bucket",
        "first_known_flag_bit1_avg_3_prior",
        F.when(F.col("key").rlike("^(version|region)$"), "GROUP_B").otherwise("GROUP_A").alias("key_zone"),
    )
    .orderBy(F.col("key_min_status_code_high_bucket").desc_nulls_first(), "key")
)
