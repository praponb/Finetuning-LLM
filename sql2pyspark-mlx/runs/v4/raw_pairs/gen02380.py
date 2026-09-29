from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("status", "channel").orderBy("discount", "id").rowsBetween(-2, -1)
w3 = Window.partitionBy("status", "customer_id").orderBy(F.col("amount").desc(), "id")
w4 = Window.partitionBy("status").orderBy("amount", "id").rowsBetween(-6, -1)
w5 = Window.partitionBy("channel").orderBy("discount", "id").rowsBetween(-2, Window.currentRow)

result = (
    df_orders
    .select(
        "*",
        F.sum("discount").over(w).alias("cumulative_discount"),
        F.avg("shipping_fee").over(w2).alias("shipping_fee_avg_2_prior"),
        F.ntile(5).over(w3).alias("amount_bucket"),
        F.avg("amount").over(w4).alias("amount_avg_6_prior"),
        F.avg("shipping_fee").over(w5).alias("shipping_fee_avg_2"),
    )
)
