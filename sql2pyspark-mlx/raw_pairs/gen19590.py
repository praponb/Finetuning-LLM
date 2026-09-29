from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("amount").asc_nulls_last(), "id")

deduped = (
    df_orders
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    deduped
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "channel",
        "discount",
        "shipping_fee",
        "rank_in_group",
        F.when(F.col("channel").rlike("^(mobile|web)$"), "PRIORITY").when(F.col("channel").rlike("^(store|partner)$"), "GROUP_B").otherwise("OTHER_ZONE").alias("channel_zone"),
    )
    .orderBy("shipping_fee")
)
