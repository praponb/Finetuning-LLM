from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("status_code").desc())
w2 = Window.partitionBy("level").orderBy("status_code")
w3 = Window.partitionBy("k", "level")

lines = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "level",
        "status_code",
        F.explode_outer("context").alias("k", "value"),
    )
)

result = (
    lines
    .select(
        "*",
        F.rank().over(w).alias("status_code_rank"),
        F.cume_dist().over(w2).alias("status_code_cume_dist"),
        F.round((F.col("status_code") / F.sum("status_code").over(w3)), 4).alias("status_code_share"),
    )
)
