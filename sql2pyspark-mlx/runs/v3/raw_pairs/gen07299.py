from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("amount").between(550, 830))
    .select("status")
    .unionByName(df_payments.select("status"))
    .distinct()
)
