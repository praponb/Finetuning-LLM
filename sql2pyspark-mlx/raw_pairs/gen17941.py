from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("order_date") <= "2023-03-11") | F.col("shipping_fee").between(14, 15))
    .select("id", "amount")
    .orderBy(F.col("id").desc(), F.col("amount").desc())
)
