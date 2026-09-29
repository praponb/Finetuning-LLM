from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method", "order_id").orderBy(F.col("amount").desc(), "id")

result = (
    df_payments
    .select(
        "id",
        "method",
        "order_id",
        "amount",
        F.row_number().over(w).alias("amount_row"),
    )
)
