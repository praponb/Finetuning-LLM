from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("order_id").orderBy("amount", "id")
w_ordered = Window.partitionBy("order_id").orderBy(F.col("amount").desc())

filtered = (
    df_payments
    .filter(F.col("order_id").isNotNull() & F.col("status").isin(["SUCCESS", "REFUNDED"]))
    .select(
        "id",
        "order_id",
        "amount",
        F.weekofyear("paid_at").alias("paid_at_weekofyear"),
        F.lower("method").alias("method_lower"),
    )
)

result = (
    filtered
    .select(
        "id",
        "order_id",
        "paid_at_weekofyear",
        "method_lower",
        F.lag("amount").over(w_running).alias("prev_amount"),
        F.lead("paid_at_weekofyear").over(w_running).alias("next_paid_at_weekofyear"),
        F.row_number().over(w_running).alias("row_seq"),
        F.rank().over(w_ordered).alias("amount_rank"),
    )
)
