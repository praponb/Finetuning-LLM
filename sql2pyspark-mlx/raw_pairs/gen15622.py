from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("flag_bit1").desc_nulls_first())

flattened = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("key", "value"),
    )
)

parsed = (
    flattened
    .select(
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("flag_bit1"),
    )
)

cleaned = (
    parsed
    .filter(F.col("status_code_high") > 2)
    .withColumn("status_code_high_whole", F.ceil("status_code_high"))
)

with_windows = cleaned.select("*", F.rank().over(w).alias("flag_bit1_rank"))

valid = (
    with_windows
    .filter(F.col("status_code_high") > 1)
    .withColumn("status_code_high_whole_whole", F.floor("status_code_high_whole"))
)

cleaned_app_logs = (
    valid
    .filter(F.col("flag_bit1_rank") <= 3)
    .withColumn("greatest_status_code_high_whole_status_code_high", F.greatest("status_code_high_whole", "status_code_high"))
    .withColumn("status_code_high_plus_status_code_high_whole", (F.col("status_code_high") + F.col("status_code_high_whole")))
)

cleaned_app_logs_2 = (
    cleaned_app_logs
    .filter(F.col("status_code_high_whole_whole").between(67, 75))
    .select(
        "status_code_high",
        "flag_bit1_rank",
        "status_code_high_whole_whole",
        "greatest_status_code_high_whole_status_code_high",
        "status_code_high_plus_status_code_high_whole",
        (F.col("flag_bit1") / F.nullif("status_code_high", F.lit(0))).alias("flag_bit1_per_status_code_high"),
        F.greatest("status_code_high_whole_whole", "greatest_status_code_high_whole_status_code_high").alias("greatest_status_code_high_whole_whole_greatest_status_code_high_whole_status_code_high"),
    )
)

result = (
    cleaned_app_logs_2
    .select(
        "status_code_high",
        "flag_bit1_rank",
        "status_code_high_whole_whole",
        "greatest_status_code_high_whole_status_code_high",
        "flag_bit1_per_status_code_high",
        "greatest_status_code_high_whole_whole_greatest_status_code_high_whole_status_code_high",
    )
)
