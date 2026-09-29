from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("amount", "id")

cleaned = (
    df_payments
    .filter((F.col("paid_at") >= F.to_timestamp(F.lit("2023-10-18 16:00:00"))) & (F.col("paid_at") < "2025-09-01"))
    .select("id", "amount", F.upper("status").alias("status_upper"))
)

result = (
    cleaned
    .select(
        "id",
        "amount",
        F.lag("amount", 1, 0).over(w).alias("prev_amount"),
        F.lead("amount", 1, 0).over(w).alias("next_amount"),
        F.lead("amount", 1, 0).over(w).alias("next_amount_2"),
    )
)
