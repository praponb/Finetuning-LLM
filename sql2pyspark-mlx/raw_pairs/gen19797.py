from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method")
w2 = Window.partitionBy("method").orderBy(F.col("amount").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_payments
    .select(
        "id",
        "method",
        "amount",
        "order_id",
        F.round((F.col("amount") / F.sum("amount").over(w)), 3).alias("amount_share"),
        F.last("order_id").over(w2).alias("last_order_id"),
    )
)
