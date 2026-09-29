from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("order_date", "id")

result = (
    df_orders
    .filter(F.col("customer_id") == 39)
    .select(
        "id",
        "store_id",
        "order_date",
        "discount",
        F.sum("discount").over(w).alias("running_discount"),
    )
)
