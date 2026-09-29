from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("order_date").desc())
w2 = Window.partitionBy("customer_id").orderBy("order_date", "id").rowsBetween(-6, Window.currentRow)

result = (
    df_orders
    .filter(F.col("store_id").isNotNull())
    .select(
        "id",
        "order_date",
        "customer_id",
        "amount",
        F.dense_rank().over(w).alias("order_date_dense_rank"),
        F.round(F.avg("amount").over(w2), 2).alias("amount_moving_avg_7"),
    )
    .orderBy(F.col("id").desc())
)
