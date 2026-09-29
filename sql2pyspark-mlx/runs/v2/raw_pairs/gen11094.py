from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("discount").desc(), "id")

result = (
    df_orders
    .filter(F.col("store_id").isNull())
    .withColumn("pos", F.row_number().over(w))
    .filter(F.col("pos") <= 1)
    .select("id", "status", "discount", "customer_id")
)
