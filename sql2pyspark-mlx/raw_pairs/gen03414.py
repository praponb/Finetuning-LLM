from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("order_date_month", "id").rowsBetween(-6, -1)
w2 = Window.orderBy("shipping_fee", "id")
w_hist = Window.partitionBy("channel").orderBy("discount", "id")

cleaned = (
    df_orders
    .filter((F.col("order_date") >= F.lit("2023-07-17").cast("date")) & (F.col("channel").eqNullSafe(F.lit("web"))))
    .select("*", F.month("order_date").alias("order_date_month"))
)

result = (
    cleaned
    .withColumn("order_date_month_avg_6_prior", F.avg("order_date_month").over(w))
    .withColumn("event_seq", F.row_number().over(w2))
    .withColumn("discount_bucket", F.ntile(5).over(w_hist))
)
