from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isNotNull())
    .select("method")
    .unionByName(df_payments.filter(F.col("amount") < 1520).select("method"))
)
