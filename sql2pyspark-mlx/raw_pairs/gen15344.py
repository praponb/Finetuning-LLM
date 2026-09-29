from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.orderBy("status_code", "id").rowsBetween(-5, -1)
w2 = Window.partitionBy("service").orderBy("status_code", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("service").orderBy(F.col("has_fault").desc(), "id")

flattened_attrs = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "status_code",
        "context",
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
        F.col("context").getItem("trace").alias("trace"),
        F.coalesce(F.col("context")["trace"], F.lit("direct")).alias("trace_or_default"),
        (F.col("status_code").bitwiseAND(8) != 0).cast("int").alias("has_fault"),
    )
)

result = (
    flattened_attrs
    .filter(F.col("status_code") > 386)
    .select(
        "id",
        "service",
        "status_code",
        "trace",
        "trace_or_default",
        "has_fault",
        F.avg("status_code").over(w_part).alias("status_code_avg_5_prior"),
        F.last("trace", ignorenulls=True).over(w2).alias("last_known_trace"),
        F.row_number().over(w_hist).alias("seq_no"),
    )
)
