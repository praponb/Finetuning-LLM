from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(F.date_format("order_date", "yyyy-MM").alias("order_month"))
    .agg(F.countDistinct("amount").alias("unique_stores"))
    .filter(F.col("unique_stores") >= 4)
)
