from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w2 = Window.partitionBy("k")

lines = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "status_code",
        F.explode_outer("context").alias("k", "value"),
    )
)

result = (
    lines
    .filter(~F.col("k").isin(["version", "region"]))
    .select(
        "id",
        "log_ts",
        "k",
        F.max("status_code").over(w).alias("status_code_max_15m"),
        F.round((F.col("status_code") / F.sum("status_code").over(w2)), 4).alias("status_code_share"),
    )
)
