from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("amount").desc())
w2 = Window.partitionBy("customer_id").orderBy("order_date", "id")

result = (
    df_orders
    .filter(F.col("store_id").isNull())
    .select(
        "id",
        "customer_id",
        "amount",
        "order_date",
        F.dense_rank().over(w).alias("amount_dense_rank"),
        F.lag("amount", 1).over(w2).alias("prev_amount"),
    )
)
