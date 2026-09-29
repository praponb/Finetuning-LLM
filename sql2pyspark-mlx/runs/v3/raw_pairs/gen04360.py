from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("started_at", "id")

result = (
    df_sessions
    .filter(F.col("browser") == "Chrome")
    .select(
        "id",
        "customer_id",
        "started_at",
        "country",
        F.row_number().over(w).alias("pos"),
    )
    .filter(F.col("pos") <= 1)
    .select("id", "customer_id", "started_at", "country")
    .orderBy(F.col("started_at").desc())
)
