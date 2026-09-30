from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w2 = Window.partitionBy(F.month("log_ts"), "service")
w3 = Window.orderBy("status_code", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy(F.year("log_ts"), "service").orderBy(F.col("status_code").desc(), "id", "attr_key")
w5 = Window.partitionBy("service").orderBy("status_code", "id", "attr_key")

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "status_code",
        F.explode("context").alias("attr_key", "v"),
    )
)

result = (
    exploded
    .filter(F.col("status_code") == 334)
    .select(
        "id",
        "log_ts",
        "level",
        "status_code",
        "attr_key",
        F.avg("status_code").over(w).alias("status_code_avg_15m"),
        F.round((F.col("status_code") / F.sum("status_code").over(w2)), 4).alias("status_code_share"),
        F.sum("status_code").over(w3).alias("cumulative_status_code"),
        F.ntile(3).over(w4).alias("status_code_bucket"),
        F.row_number().over(w5).alias("seq_no"),
    )
)
