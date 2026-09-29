from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("shipping_fee", "id")

result = (
    df_orders
    .filter(F.col("discount") <= 0.28)
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)
