from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("service")
w_part = Window.orderBy(F.col("status_code").desc())
w3 = Window.partitionBy("level")
w4 = Window.partitionBy("level", "k")

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "status_code",
        F.explode_outer("context").alias("k", "value"),
    )
)

result = (
    lines
    .select(
        "host",
        "status_code",
        "k",
        "value",
        F.max("status_code").over(w_grp).alias("service_max_status_code"),
        F.cume_dist().over(w_part).alias("status_code_cume_dist"),
        F.percent_rank().over(w_part).alias("status_code_percent_rank"),
        F.round((F.col("status_code") / F.sum("status_code").over(w3)), 4).alias("status_code_share"),
        F.round((F.col("status_code") / F.sum("status_code").over(w4)), 4).alias("status_code_share_2"),
    )
)
