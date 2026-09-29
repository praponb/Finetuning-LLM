from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("YEAR", "started_at"), "customer_id").orderBy("started_at", "id")
w2 = Window.partitionBy("browser").orderBy("started_at", "id")

result = (
    df_sessions
    .filter(F.col("device") == "tablet")
    .select(
        "id",
        "customer_id",
        "device",
        "started_at",
        F.row_number().over(w).alias("event_seq"),
        F.row_number().over(w2).alias("row_seq"),
    )
)
