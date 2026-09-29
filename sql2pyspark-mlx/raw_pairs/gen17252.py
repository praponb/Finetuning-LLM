from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("status") == "COMPLETED") | (F.col("order_date") <= "2024-03-01"))
    .withColumn("status_channel", F.concat("status", F.lit(" "), "channel"))
    .withColumn("shipping_fee_whole", F.floor("shipping_fee"))
    .orderBy(F.col("shipping_fee_whole").desc())
)
