from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("WEEK", "started_at"), "device").orderBy(F.col("started_at").desc(), "id")
w2 = Window.partitionBy("browser").orderBy(F.col("started_at").desc(), "id")
w3 = Window.partitionBy("device", "country").orderBy(F.col("started_at").desc(), "id")

result = (
    df_sessions
    .filter(F.col("browser").isin(["Chrome", "Firefox"]))
    .select(
        "id",
        "customer_id",
        "device",
        F.row_number().over(w).alias("event_seq"),
        F.row_number().over(w2).alias("event_seq_2"),
        F.row_number().over(w3).alias("event_seq_3"),
    )
)
