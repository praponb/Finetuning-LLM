from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w_part = Window.partitionBy("attr_key").orderBy("status_code_high", "id", "attr_key")
w_hist = Window.partitionBy("host").orderBy("next_status_code_high", "id", "attr_key")
w4 = Window.partitionBy("service", "attr_key").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w5 = Window.orderBy(F.col("next_status_code_high").desc(), "id", "attr_key")

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "status_code",
        F.explode("context").alias("attr_key", "attr_value"),
    )
)

flattened_attrs = (
    lines
    .filter(F.col("host") == "h3")
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "status_code",
        "attr_key",
        "attr_value",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(8) != 0).cast("int").alias("is_flagged"),
    )
)

with_windows = (
    flattened_attrs
    .withColumn("status_code_high_sum_2h", F.sum("status_code_high").over(w))
    .withColumn("next_status_code_high", F.lead("status_code_high", 2).over(w_part))
)

result = (
    with_windows
    .withColumn("prev_next_status_code_high", F.lag("next_status_code_high", 1, 0).over(w_hist))
    .withColumn("status_code_high_sum_2h_sum_15m", F.sum("status_code_high_sum_2h").over(w4))
    .withColumn("next_status_code_high_bucket", F.ntile(4).over(w5))
)
