from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service")

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "status_code",
        F.explode_outer("context").alias("k", "value"),
    )
)

result = (
    lines
    .filter(F.col("k") == "version")
    .select(
        "id",
        "status_code",
        "k",
        F.round((F.col("status_code") / F.sum("status_code").over(w)), 4).alias("status_code_share"),
    )
)
