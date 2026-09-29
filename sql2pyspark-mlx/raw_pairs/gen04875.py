from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("started_at").desc())

result = (
    df_sessions
    .filter(F.col("country") == "UK")
    .select(
        "id",
        "customer_id",
        "started_at",
        "country",
        F.dense_rank().over(w).alias("row_num"),
    )
    .filter(F.col("row_num") == 1)
    .select("id", "customer_id", "started_at", "country")
)
