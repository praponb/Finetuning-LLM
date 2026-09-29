from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("customer_id").orderBy("shipping_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("discount", "id").rowsBetween(-2, -1)
w4 = Window.partitionBy("channel", "customer_id").orderBy("amount", "id").rowsBetween(-5, -1)
w_ordered = Window.partitionBy(F.year("order_date")).orderBy(F.col("amount").asc_nulls_first())

scored = (
    df_orders
    .withColumn("running_amount", F.sum("amount").over(w))
    .withColumn("last_known_shipping_fee", F.last("shipping_fee", ignorenulls=True).over(w2))
    .withColumn("shipping_fee_avg_2_prior", F.avg("shipping_fee").over(w3))
    .withColumn("shipping_fee_avg_5_prior", F.avg("shipping_fee").over(w4))
    .withColumn("amount_cume_dist", F.cume_dist().over(w_ordered))
)

result = (
    scored
    .groupBy(F.date_trunc("week", "order_date").alias("week_start"))
    .agg(
        F.sum("amount_cume_dist").alias("total_amount_cume_dist"),
        F.size(F.collect_set("channel")).alias("num_channels"),
        F.percentile_approx("running_amount", 0.95).alias("running_amount_p95"),
        F.avg("last_known_shipping_fee").alias("avg_last_known_shipping_fee"),
    )
    .filter(F.col("total_amount_cume_dist") > 0)
    .orderBy(F.col("total_amount_cume_dist").desc_nulls_last())
)
