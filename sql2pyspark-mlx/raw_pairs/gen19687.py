from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("k", "level").orderBy("log_ts", "id", "k")
w2 = Window.partitionBy("level").orderBy("latency_ms", "id", "k")

flattened = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("k", "v"),
    )
)

result = (
    flattened
    .select(
        "id",
        "log_ts",
        "status_code",
        "k",
        F.lead("status_code").over(w_ordered).alias("next_status_code"),
        F.lag("latency_ms").over(w_ordered).alias("prev_latency_ms"),
        F.lead("latency_ms").over(w_ordered).alias("next_latency_ms"),
        F.row_number().over(w_ordered).alias("event_seq"),
        F.lead("status_code", 1).over(w2).alias("next_status_code_2"),
    )
)
