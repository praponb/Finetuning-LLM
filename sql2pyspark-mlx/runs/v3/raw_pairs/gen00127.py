from pyspark.sql import functions as F

result = (
    df_orders
    .filter(~(F.col("shipping_fee").between(30, 39) & F.col("order_date").between("2023-03-01", "2024-02-01")))
    .groupBy(
        F.when(F.col("shipping_fee") >= 30, "high").when(F.col("shipping_fee") >= 6, "mid").otherwise("low").alias("shipping_fee_bucket"),
    )
    .agg(
        F.sum("amount").alias("total_amount"),
        (F.max("amount") - F.min("amount")).alias("amount_range"),
    )
    .orderBy("total_amount")
)
