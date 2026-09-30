from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("amount", "id")

result = (
    df_orders
    .filter(F.col("order_date").between("2024-03-01", "2025-07-01"))
    .withColumn("pos", F.row_number().over(w))
    .filter(F.col("pos") <= 2)
    .select("id", "customer_id", "amount", "channel")
    .orderBy("channel")
)
