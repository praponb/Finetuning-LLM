from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("order_date").desc(), "id")

result = (
    df_orders
    .filter(~F.col("status").isin(["COMPLETED", "PENDING", "CANCELLED"]))
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") <= 2)
    .select("id", "customer_id", "order_date", "status")
    .orderBy("order_date")
)
