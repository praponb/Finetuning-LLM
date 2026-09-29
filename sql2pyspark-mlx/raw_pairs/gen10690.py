from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("started_at", "id")

result = (
    df_sessions
    .filter(F.hour("started_at").between(7, 14))
    .select(
        "id",
        "customer_id",
        "started_at",
        "browser",
        F.row_number().over(w).alias("pos"),
    )
    .filter(F.col("pos") <= 1)
    .select("id", "customer_id", "started_at", "browser")
    .orderBy(F.col("customer_id").desc())
)
