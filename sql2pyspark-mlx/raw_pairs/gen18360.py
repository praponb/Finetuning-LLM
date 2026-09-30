from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("shipping_fee_tier").orderBy(F.col("shipping_fee_stddev").desc())

not_flagged = (
    df_orders
    .alias("s")
    .join(df_order_items.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.order_id").isin([17, 33, 37, 40])), "left_semi")
)

result = (
    not_flagged
    .groupBy(
        F.when(F.col("shipping_fee") >= 41, "PREMIUM").otherwise("STANDARD").alias("shipping_fee_tier"),
        F.date_trunc("WEEK", "order_date").alias("week_start"),
        F.date_trunc("YEAR", "order_date").alias("year_start"),
    )
    .agg(
        F.max(F.when(F.col("status") == "SHIPPED", F.col("amount"))).alias("shipped_max_amount"),
        F.size(F.collect_set("status")).alias("num_status"),
        F.stddev_samp("shipping_fee").alias("shipping_fee_stddev"),
    )
    .withColumn("shipping_fee_stddev_rank", F.rank().over(w))
)
