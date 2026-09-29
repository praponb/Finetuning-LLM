from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("order_date").desc(), "id")

result = (
    df_orders
    .filter(F.col("order_date") < "2023-08-01")
    .withColumn("rnk", F.row_number().over(w))
    .filter(F.col("rnk") <= 5)
    .select("id", "status", "order_date", "shipping_fee")
)
