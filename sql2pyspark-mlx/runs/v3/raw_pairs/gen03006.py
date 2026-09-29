from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("order_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("status").orderBy(F.col("shipping_fee").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_orders
    .filter(F.col("amount").between(690, 720))
    .select(
        "id",
        "customer_id",
        "order_date",
        "amount",
        "status",
        "shipping_fee",
        F.sum("amount").over(w).alias("running_amount"),
        F.last("amount").over(w2).alias("last_amount"),
    )
)
