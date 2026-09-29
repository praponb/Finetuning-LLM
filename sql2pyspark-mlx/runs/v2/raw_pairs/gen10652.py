from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("order_date").desc(), "id")

result = (
    df_orders
    .filter(F.col("store_id") == 33)
    .withColumn("pos", F.row_number().over(w))
    .filter(F.col("pos") <= 3)
    .orderBy("status")
)
