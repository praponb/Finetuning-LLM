from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("amount").desc(), "id")

result = (
    df_orders
    .filter(F.col("order_date") < "2024-11-01")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") <= 5)
    .orderBy("order_date")
)
