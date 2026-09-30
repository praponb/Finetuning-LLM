from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k", "service").orderBy("log_ts", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("host").orderBy("status_code", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "status_code",
        F.explode("context").alias("k", "attr_value"),
    )
)

result = (
    exploded
    .filter(F.col("level") == "DEBUG")
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "k",
        "attr_value",
        F.first("k", ignorenulls=True).over(w).alias("first_known_k"),
        F.sum("status_code").over(w2).alias("running_status_code"),
    )
)
