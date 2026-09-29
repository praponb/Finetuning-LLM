from pyspark.sql import functions as F

high_value = (
    df_orders
    .groupBy("customer_id")
    .agg(F.sum("amount").alias("total"))
    .filter(F.col("total") > 2000)
)

result = high_value
