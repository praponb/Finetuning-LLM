from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("amount").desc())
w2 = Window.partitionBy("customer_id").orderBy("order_date", "id")

result = (
    df_orders
    .select(
        "id",
        "customer_id",
        "amount",
        "order_date",
        "discount",
        F.rank().over(w).alias("rnk"),
        F.lag("discount", 1).over(w2).alias("prev_discount"),
    )
)
