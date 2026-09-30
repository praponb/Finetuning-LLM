from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method") == "paypal")
    .select("amount")
    .subtract(
        df_order_items
        .filter(F.col("unit_price").between(110, 380))
        .select("unit_price")
    )
)
