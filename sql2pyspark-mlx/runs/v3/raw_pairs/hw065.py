from pyspark.sql import functions as F

result = df_orders.select(
    "id",
    F.year("order_date").alias("yr"),
    F.month("order_date").alias("mo"),
    F.dayofmonth("order_date").alias("dy"),
)
