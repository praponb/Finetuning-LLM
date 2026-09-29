from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("amount").desc(), "id")

result = (
    df_payments
    .select(
        "id",
        "order_id",
        "amount",
        "method",
        F.row_number().over(w).alias("row_num"),
    )
    .filter(F.col("row_num") <= 2)
)
