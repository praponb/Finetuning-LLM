from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("customer_id").isin([12, 34]))
    .select("status")
    .unionByName(df_accounts.select("status"))
)
