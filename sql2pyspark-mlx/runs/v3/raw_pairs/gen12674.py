from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("started_at")
w2 = Window.partitionBy("customer_id").orderBy("started_at", "id")

result = (
    df_sessions
    .filter(F.col("browser").isin(["Firefox", "Edge"]))
    .select(
        "id",
        "country",
        "started_at",
        "customer_id",
        F.dense_rank().over(w).alias("dense_rnk"),
        F.row_number().over(w2).alias("rn"),
    )
)
