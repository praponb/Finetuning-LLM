from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("discount", "id")

result = (
    df_orders
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") <= 3)
    .select("id", "channel", "discount", "amount")
    .orderBy(F.col("id").desc())
)
