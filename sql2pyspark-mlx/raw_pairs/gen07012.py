from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee") >= 25)
    .select("status")
    .unionByName(df_payments.select("status"))
    .distinct()
    .orderBy("status")
)
