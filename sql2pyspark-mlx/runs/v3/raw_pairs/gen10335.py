from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("order_date", "id")

result = (
    df_orders
    .select(
        "id",
        "order_date",
        "discount",
        (F.col("discount") - F.lag("discount", 1).over(w)).alias("discount_change"),
    )
    .orderBy("id")
)
