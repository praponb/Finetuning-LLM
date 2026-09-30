from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("order_date")

result = (
    df_orders
    .filter(F.col("shipping_fee") <= 24)
    .select(
        "id",
        "customer_id",
        "order_date",
        F.rank().over(w).alias("order_date_rank"),
    )
    .orderBy(F.col("order_date").desc())
)
