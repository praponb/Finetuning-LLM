from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method", "order_id").orderBy("amount", "id")

cleaned = (
    df_payments
    .filter(F.col("order_id").isNotNull())
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        F.to_date("paid_at").alias("paid_at_date"),
        F.floor("amount").alias("amount_whole"),
        F.coalesce("method", F.lit("UNKNOWN")).alias("method_filled"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "amount",
        "amount_whole",
        "method_filled",
        F.lag("amount").over(w).alias("prev_amount"),
        F.lead("amount_whole").over(w).alias("next_amount_whole"),
        F.lag("amount_whole").over(w).alias("prev_amount_whole"),
        F.row_number().over(w).alias("seq_no"),
    )
)
