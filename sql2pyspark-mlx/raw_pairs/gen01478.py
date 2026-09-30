from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.year("paid_at"), "order_id")
w_part = Window.partitionBy("method").orderBy("paid_at", "id")
w3 = Window.partitionBy("method").orderBy("amount", "id")
w_recent = Window.partitionBy("status", "order_id").orderBy("amount", "id")

result = (
    df_payments
    .select(
        "id",
        "status",
        F.round((F.col("amount") / F.sum("amount").over(w)), 4).alias("amount_share"),
        F.row_number().over(w_part).alias("event_seq"),
        F.row_number().over(w_part).alias("seq_no"),
        F.row_number().over(w3).alias("row_seq"),
        F.row_number().over(w_recent).alias("row_seq_2"),
    )
)
