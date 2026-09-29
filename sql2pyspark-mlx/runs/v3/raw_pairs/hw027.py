from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(F.year("order_date").alias("yr"), F.month("order_date").alias("mo"))
    .agg(F.count("*").alias("order_count"))
)
