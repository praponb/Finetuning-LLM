from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("started_at").desc(), "id")

result = (
    df_sessions
    .filter(F.col("browser") != "Edge")
    .select(
        "id",
        "customer_id",
        "started_at",
        "device",
        F.row_number().over(w).alias("pos"),
    )
    .filter(F.col("pos") <= 3)
    .orderBy("device")
)
