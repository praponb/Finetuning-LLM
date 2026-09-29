from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("order_date", "id")

ranked = (
    df_orders
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "customer_id", "status", "channel", "discount", "shipping_fee")
)

result = (
    ranked
    .select(
        "id",
        "customer_id",
        "status",
        "channel",
        F.when(F.col("status").rlike("^(COMPLETED|SHIPPED)$"), "CORE").otherwise("PRIORITY").alias("status_zone"),
    )
    .orderBy(F.col("status_zone").desc_nulls_last(), "id")
)
