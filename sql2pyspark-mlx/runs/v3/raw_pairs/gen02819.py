from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("paid_at").desc(), "id")
w2 = Window.partitionBy("method").orderBy(F.col("paid_at").desc(), "id")

result = (
    df_payments
    .select(
        "id",
        "order_id",
        "paid_at",
        "method",
        F.row_number().over(w).alias("rn"),
        F.row_number().over(w2).alias("paid_at_row"),
    )
)
