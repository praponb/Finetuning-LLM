from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("shipping_fee") > 21) & (F.col("discount") < 0.38))
    .select("discount", "store_id")
    .orderBy("discount")
)
