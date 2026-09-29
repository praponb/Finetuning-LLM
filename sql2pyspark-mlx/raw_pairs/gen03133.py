from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("status").orderBy("paid_at", "id")
w2 = Window.partitionBy("status").orderBy(F.col("paid_at").desc(), "id")

valid = (
    df_payments
    .filter((F.col("paid_at") >= F.lit("2023-11-14 14:00:00").cast("timestamp")) & (F.col("method").eqNullSafe(F.lit("card"))))
    .select(
        "id",
        "paid_at",
        "status",
        F.concat("method", F.lit(", "), "status").alias("method_status"),
    )
)

result = (
    valid
    .select(
        "id",
        "paid_at",
        "status",
        "method_status",
        F.row_number().over(w_running).alias("seq_no"),
        F.row_number().over(w_running).alias("row_seq"),
        F.row_number().over(w2).alias("seq_no_2"),
        F.row_number().over(w_running).alias("event_seq"),
    )
)
