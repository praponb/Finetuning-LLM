from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isNotNull())
    .select("status")
    .intersect(df_payments.filter(F.col("order_id") == 28).select("status"))
)
