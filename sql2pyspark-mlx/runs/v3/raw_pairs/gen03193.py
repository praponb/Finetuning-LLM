from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("started_at").desc())

result = (
    df_sessions
    .filter(F.col("customer_id").isin([8, 15, 39]))
    .select(
        "id",
        "customer_id",
        "started_at",
        F.dense_rank().over(w).alias("dense_rnk"),
    )
)
