from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("channel") == "store") & F.col("order_date").between("2024-03-01", "2024-11-01"))
    .withColumn("discount_plus_amount", (F.col("discount") + F.col("amount")))
    .orderBy(F.col("id").desc())
    .limit(10)
)
