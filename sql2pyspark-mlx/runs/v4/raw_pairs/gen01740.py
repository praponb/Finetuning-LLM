from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.orderBy("num_context", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("status_code", "id").rowsBetween(-5, -1)
w3 = Window.orderBy("num_context", "id")

flattened_attrs = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "status_code",
        "context",
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
        F.col("context")["trace"].alias("trace"),
        F.size("context").alias("num_context"),
    )
)

result = (
    flattened_attrs
    .select(
        "*",
        F.first("num_context", ignorenulls=True).over(w_running).alias("first_known_num_context"),
        F.first("num_context", ignorenulls=True).over(w_running).alias("first_known_num_context_2"),
        F.avg("num_context").over(w2).alias("num_context_avg_5_prior"),
        F.lag("num_context", 1).over(w3).alias("prev_num_context"),
    )
)
