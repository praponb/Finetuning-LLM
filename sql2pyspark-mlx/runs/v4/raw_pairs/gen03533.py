from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("shipping_fee").desc(), "id")

ranked = (
    df_orders
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    ranked
    .groupBy(
        F.when(F.col("amount") >= 1360, "LARGE").otherwise("SMALL").alias("amount_tier"),
        "channel",
    )
    .agg(
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
        F.sum(F.when(F.col("channel") == "mobile", F.col("shipping_fee"))).alias("mobile_total_shipping_fee"),
        F.size(F.collect_set("channel")).alias("num_channels"),
    )
    .filter(F.col("mobile_total_shipping_fee") > 0)
    .orderBy(F.col("amount_tier").desc())
)
