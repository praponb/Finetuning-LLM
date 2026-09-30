from pyspark.sql import functions as F

result = (
    df_payments
    .select("amount")
    .union(df_store_sales.select("revenue"))
    .distinct()
    .orderBy("amount")
)
