from pyspark.sql import functions as F

result = (
    df_products
    .select("cost")
    .union(df_payments.select("amount"))
    .distinct()
)
