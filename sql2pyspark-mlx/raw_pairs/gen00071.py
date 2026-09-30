from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("order_id").orderBy("paid_at", "id")

scored = (
    df_payments
    .filter(F.col("amount") > 600)
    .select(
        "*",
        F.lag("amount").over(w_recent).alias("prev_amount"),
        F.lead("amount").over(w_recent).alias("next_amount"),
        F.row_number().over(w_recent).alias("seq_no"),
    )
)

result = (
    scored
    .select("id", "order_id", "amount", "status", "next_amount", "seq_no")
    .orderBy(F.col("status").asc_nulls_first())
)
