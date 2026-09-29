from pyspark.sql import functions as F

result = (
    df_orders
    .select("status")
    .unionByName(df_payments.select("status"))
    .orderBy("status")
)
