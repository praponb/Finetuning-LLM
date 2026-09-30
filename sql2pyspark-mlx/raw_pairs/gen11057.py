from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "customer_id").orderBy("discount")

deduped = (
    df_orders
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
)

result = (
    deduped
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "channel",
        "amount",
        F.when(F.col("channel").rlike("^(partner|mobile)$"), "OTHER_ZONE").when(F.col("channel").rlike("^(web|store)$"), "PRIORITY").otherwise("EXTENDED").alias("channel_zone"),
    )
    .orderBy(F.col("channel_zone").desc())
)
