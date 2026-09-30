from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy(F.col("started_at").desc(), "id")

result = (
    df_sessions
    .filter(~F.col("device").isin(["desktop", "tablet", "mobile"]))
    .select(
        "id",
        "device",
        "started_at",
        "customer_id",
        F.row_number().over(w).alias("rnk"),
    )
    .filter(F.col("rnk") <= 2)
    .select("id", "device", "started_at", "customer_id")
    .orderBy("started_at")
)
