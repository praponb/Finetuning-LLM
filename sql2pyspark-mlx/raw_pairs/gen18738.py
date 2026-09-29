from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("status").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy(F.year("order_date"), "customer_id").orderBy("discount")

result = (
    df_orders
    .select(
        "*",
        F.sum("discount").over(w_running).alias("running_sum_discount"),
        F.count("discount").over(w_running).alias("running_count_discount"),
        F.avg("discount").over(w_running).alias("running_avg_discount"),
        F.percent_rank().over(w2).alias("discount_percent_rank"),
    )
)
