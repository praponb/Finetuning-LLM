from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("amount").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w2 = Window.partitionBy("status").orderBy("order_date")

result = (
    df_orders
    .select(
        "id",
        "customer_id",
        "amount",
        "status",
        "order_date",
        F.last("status").over(w).alias("last_status"),
        F.rank().over(w2).alias("rnk"),
    )
)
