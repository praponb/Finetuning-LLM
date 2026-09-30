from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("order_date").desc())
w2 = Window.partitionBy("store_id").orderBy("order_date", "id")

result = (
    df_orders
    .select(
        "id",
        "order_date",
        "store_id",
        "amount",
        F.dense_rank().over(w).alias("dense_rnk"),
        F.sum("amount").over(w2).alias("running_amount"),
    )
)
