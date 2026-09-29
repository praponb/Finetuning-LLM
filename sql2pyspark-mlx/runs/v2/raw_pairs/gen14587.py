from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("started_at").desc())

result = (
    df_sessions
    .filter(F.col("customer_id") == 15)
    .select(
        "id",
        "country",
        "started_at",
        F.dense_rank().over(w).alias("started_at_dense_rank"),
    )
)
