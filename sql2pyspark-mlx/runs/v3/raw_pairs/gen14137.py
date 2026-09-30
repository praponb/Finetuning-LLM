from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(F.year("order_date").alias("order_year"))
    .agg(F.max("shipping_fee").alias("max_shipping_fee"))
    .filter(F.col("max_shipping_fee") > 16)
    .select("order_year", "max_shipping_fee")
    .orderBy("order_year")
)
