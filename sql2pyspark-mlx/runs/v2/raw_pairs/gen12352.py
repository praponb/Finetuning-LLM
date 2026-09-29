from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("discount", "id")

result = (
    df_orders
    .filter(~F.col("status").isin(["PENDING", "RETURNED"]))
    .select(
        "id",
        "customer_id",
        "discount",
        "order_date",
        F.first("order_date").over(w).alias("first_order_date"),
    )
    .orderBy(F.col("customer_id").desc())
)
