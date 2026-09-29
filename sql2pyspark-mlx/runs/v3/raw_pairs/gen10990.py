from pyspark.sql import functions as F

result = (
    df_transactions
    .select("amount")
    .unionByName(
        df_order_items
        .filter(F.col("unit_price").between(220, 340))
        .select(F.col("unit_price").alias("amount"))
    )
    .orderBy("amount")
)
