from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("amount") <= 910)
    .withColumn("amount_minus_discount", (F.col("amount") - F.col("discount")))
)
