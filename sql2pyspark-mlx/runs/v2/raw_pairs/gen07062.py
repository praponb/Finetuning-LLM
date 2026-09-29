from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("paid_at").desc())

result = (
    df_payments
    .select(
        "id",
        "order_id",
        "paid_at",
        "amount",
        F.dense_rank().over(w).alias("row_num"),
    )
    .filter(F.col("row_num") == 1)
    .orderBy(F.col("row_num").desc())
)
