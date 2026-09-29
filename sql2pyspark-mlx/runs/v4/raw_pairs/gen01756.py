from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("order_date", "id")

result = (
    df_orders
    .filter(F.col("discount") > 0.25)
    .withColumn("pos", F.row_number().over(w))
    .filter(F.col("pos") <= 1)
    .select("id", "store_id", "order_date", "customer_id")
)
