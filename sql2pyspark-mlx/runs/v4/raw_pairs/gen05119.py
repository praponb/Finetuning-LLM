from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level", "key").orderBy(F.col("has_fault").desc(), "id", "key")
w2 = Window.partitionBy("key").orderBy("has_fault", "id", "key")
w3 = Window.partitionBy("key").orderBy("has_fault", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("has_fault", "id", "key")
w5 = Window.partitionBy("level").orderBy("prev_status_code_high", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("level").orderBy("cumulative_has_fault", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "status_code",
        F.explode("context").alias("key", "attr_value"),
    )
)

parsed = (
    lines
    .select(
        "id",
        "level",
        "key",
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("has_fault"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

deduped = (
    parsed
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

latest = (
    deduped
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

enriched = (
    latest
    .select(
        "*",
        F.sum("has_fault").over(w3).alias("cumulative_has_fault"),
        F.lag("status_code_high", 1).over(w4).alias("prev_status_code_high"),
    )
)

enriched_app_logs = (
    enriched
    .withColumn("running_status_code_high", F.sum("status_code_high").over(w5))
    .withColumn("running_has_fault", F.sum("has_fault").over(w6))
)

result = (
    enriched_app_logs
    .select(
        "id",
        "key",
        "has_fault",
        "status_code_high",
        "cumulative_has_fault",
        "running_has_fault",
    )
    .orderBy(F.col("status_code_high").desc_nulls_first())
)
