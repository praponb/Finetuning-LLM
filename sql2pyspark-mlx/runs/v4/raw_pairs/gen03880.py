from pyspark.sql import functions as F

result = (
    df_payments
    .filter(~F.col("method").isin(["promptpay", "bank_transfer"]))
    .select("amount")
    .union(df_products.select("price"))
    .distinct()
)
