from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("amount", "id")

result = (
    df_payments
    .select(
        "id",
        "method",
        "amount",
        "order_id",
        F.row_number().over(w).alias("row_num"),
    )
    .filter(F.col("row_num") == 1)
    .select("id", "method", "amount", "order_id")
)
