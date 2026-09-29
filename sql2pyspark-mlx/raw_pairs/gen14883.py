from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key")
w2 = Window.partitionBy("level")

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "level",
        "status_code",
        F.explode("context").alias("attr_key", "value"),
    )
)

result = (
    exploded
    .filter(F.col("level").isin(["ERROR", "INFO"]))
    .select(
        "*",
        F.round((F.col("status_code") / F.sum("status_code").over(w)), 4).alias("status_code_share"),
        F.round((F.col("status_code") / F.sum("status_code").over(w2)), 4).alias("status_code_share_2"),
        F.round((F.col("status_code") / F.sum("status_code").over(w2)), 4).alias("status_code_share_3"),
    )
)
