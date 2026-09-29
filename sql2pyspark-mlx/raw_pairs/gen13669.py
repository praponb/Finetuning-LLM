from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("discount").asc_nulls_last(), "id")

ranked = (
    df_orders
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    ranked
    .withColumn("activity_month", F.date_trunc("MONTH", "order_date"))
    .cube("customer_id", "activity_month")
    .agg(
        F.corr("shipping_fee", "amount").alias("corr_shipping_fee_amount"),
        F.corr("discount", "amount").alias("corr_discount_amount"),
        F.round(F.sum("discount") / F.nullif(F.count(F.when(F.col("channel") == "store", 1)), F.lit(0)), 2).alias("discount_per_store"),
        F.round(F.avg("shipping_fee"), 2).alias("avg_shipping_fee"),
        F.count_if(F.col("status") == "PENDING").alias("pending_events"),
        F.corr("amount", "shipping_fee").alias("corr_amount_shipping_fee"),
        F.grouping("customer_id").alias("grouping_id"),
    )
    .orderBy(F.col("corr_amount_shipping_fee").desc_nulls_last())
)
