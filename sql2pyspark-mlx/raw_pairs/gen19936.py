from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("order_date", "id").rowsBetween(-3, Window.currentRow)

result = (
    df_orders
    .filter(F.col("customer_id").isNull())
    .select(
        "id",
        "status",
        "order_date",
        "amount",
        F.round(F.avg("amount").over(w), 2).alias("amount_moving_avg_4"),
    )
)
