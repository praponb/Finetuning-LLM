from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("order_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("status").orderBy(F.col("amount").desc())

result = (
    df_orders
    .select(
        "id",
        "customer_id",
        "order_date",
        "discount",
        "status",
        "amount",
        F.sum("discount").over(w).alias("running_discount"),
        F.rank().over(w2).alias("rnk"),
    )
)
