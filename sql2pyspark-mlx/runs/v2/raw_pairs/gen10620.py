from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("amount") < 730) | F.col("status").isin(["PENDING", "CANCELLED"]))
    .withColumn("order_date_plus_months", F.add_months("order_date", 12))
    .withColumn("clean_status", F.trim("status"))
    .orderBy("order_date", "id")
    .limit(20)
)
