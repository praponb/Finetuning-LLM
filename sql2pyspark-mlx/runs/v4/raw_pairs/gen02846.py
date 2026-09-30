from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k").orderBy("log_ts", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("host", "service").orderBy("log_ts", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("service", "k").orderBy("log_ts", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("k").orderBy("log_ts", "id", "k")
w5 = Window.partitionBy("service").orderBy(F.col("log_ts").desc(), "id", "k")

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        F.explode("context").alias("k", "attr_value"),
    )
)

result = (
    exploded
    .select(
        "*",
        F.first("k", ignorenulls=True).over(w).alias("first_known_k"),
        F.last("k", ignorenulls=True).over(w2).alias("last_known_k"),
        F.first("k", ignorenulls=True).over(w3).alias("first_known_k_2"),
        F.row_number().over(w4).alias("event_seq"),
        F.row_number().over(w5).alias("event_seq_2"),
    )
)
