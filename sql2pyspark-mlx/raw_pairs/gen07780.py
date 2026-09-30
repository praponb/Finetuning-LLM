from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price").isNotNull())
    .select("unit_price")
    .subtract(df_payments.filter(F.col("amount").isNotNull()).select("amount"))
    .orderBy("unit_price")
)
