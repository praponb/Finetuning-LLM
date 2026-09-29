from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("order_date", "id")

result = (
    df_orders
    .filter(F.col("customer_id") == 4)
    .select(
        "id",
        "order_date",
        "discount",
        F.lead("discount", 1).over(w).alias("next_discount"),
    )
    .orderBy("order_date")
)
