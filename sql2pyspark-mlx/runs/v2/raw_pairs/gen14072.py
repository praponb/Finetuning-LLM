from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("status") == "REFUNDED")
    .select("method")
    .unionByName(df_payments.filter(F.col("order_id").isNull()).select("method"))
)
