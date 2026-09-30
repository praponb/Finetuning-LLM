from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("customer_id").isin([3, 11, 26]) | (F.col("order_date") <= "2023-09-05"))
    .withColumn("amount_level", F.when(F.col("amount") >= 1550, "High").when(F.col("amount") >= 540, "Medium").otherwise("Low"))
    .withColumn("status_upper", F.upper("status"))
    .orderBy("status", "store_id")
)
