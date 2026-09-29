from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.year("order_date") == 2025)
    .groupBy("channel")
    .agg(F.countDistinct("shipping_fee").alias("unique_stores"))
    .filter(F.col("unique_stores") >= 5)
    .select("channel", "unique_stores")
)
