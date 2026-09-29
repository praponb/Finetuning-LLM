from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("order_date", "id")

result = (
    df_orders
    .filter(F.col("store_id").isin([9, 34, 39]))
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "channel", "order_date")
    .orderBy(F.col("channel").desc())
)
