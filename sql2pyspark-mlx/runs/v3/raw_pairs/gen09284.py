from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("order_date", "id")

result = (
    df_orders
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "order_date",
        "discount",
        F.lag("discount", 1).over(w).alias("prev_discount"),
    )
    .orderBy(F.col("order_date").desc())
)
