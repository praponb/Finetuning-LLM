from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("discount").desc_nulls_first(), "id")

ranked = (
    df_orders
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

result = (
    ranked
    .groupBy("status", F.date_trunc("QUARTER", "order_date").alias("quarter_start"))
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.percentile_approx("discount", 0.9).alias("discount_p90"),
        F.sum(F.when(F.col("status") == "PENDING", F.col("shipping_fee")).otherwise(0)).alias("pending_shipping_fee"),
        F.round(F.sum("shipping_fee") / F.nullif(F.count(F.when(F.col("channel") == "mobile", 1)), F.lit(0)), 2).alias("shipping_fee_per_mobile"),
        F.sum(F.when(F.col("status") == "PENDING", F.col("amount")).otherwise(0)).alias("pending_amount"),
    )
    .filter(F.col("pending_shipping_fee") >= 0)
    .orderBy(F.col("avg_amount").desc_nulls_last())
)
