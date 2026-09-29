from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("channel") != "store")
    .select("customer_id")
    .unionByName(df_orders.filter(F.col("customer_id").isNull()).select("customer_id"))
    .orderBy("customer_id")
)
