from pyspark.sql import functions as F

result = (
    df_transactions
    .select("amount")
    .subtract(df_order_items.select("unit_price"))
    .orderBy(F.col("amount").desc())
)
