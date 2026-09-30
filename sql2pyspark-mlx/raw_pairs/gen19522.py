from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("status_code").desc())
w2 = Window.partitionBy("service").orderBy(F.col("status_code").desc())
w3 = Window.partitionBy("service", "key")

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "status_code",
        F.explode_outer("context").alias("key", "v"),
    )
)

result = (
    flattened
    .select(
        "id",
        "service",
        F.cume_dist().over(w).alias("status_code_cume_dist"),
        F.dense_rank().over(w2).alias("status_code_dense_rank"),
        F.dense_rank().over(w).alias("status_code_dense_rank_2"),
        F.round((F.col("status_code") / F.sum("status_code").over(w3)), 4).alias("status_code_share"),
    )
)
