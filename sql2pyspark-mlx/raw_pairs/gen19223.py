from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price").isNotNull())
    .select(F.col("unit_price").alias("value"))
    .subtract(
        df_transactions
        .filter(F.col("amount").isNotNull())
        .select(F.col("amount").alias("value"))
    )
    .orderBy("value")
)
