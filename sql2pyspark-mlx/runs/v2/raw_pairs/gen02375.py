from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("delivered_date", "id")

result = (
    df_shipments
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") <= 3)
    .select("id", "order_id", "delivered_date")
)
