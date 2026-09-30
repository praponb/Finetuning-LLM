from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("discount").desc(), "id")

result = (
    df_orders
    .withColumn("pos", F.row_number().over(w))
    .filter(F.col("pos") <= 3)
    .select("id", "channel", "discount", "customer_id")
    .orderBy("id", F.col("channel").desc())
)
