from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("order_date", "id")

result = (
    df_orders
    .filter(F.col("channel").isin(["store", "partner", "web"]))
    .select(
        "id",
        "order_date",
        "discount",
        (F.col("discount") - F.lag("discount", 2).over(w)).alias("discount_change"),
    )
)
