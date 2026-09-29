from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("order_date", "id").rowsBetween(-3, Window.currentRow)

result = (
    df_orders
    .select(
        "id",
        "status",
        "order_date",
        "discount",
        F.round(F.avg("discount").over(w), 2).alias("discount_moving_avg_4"),
    )
)
