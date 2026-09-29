from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("discount").desc(), "id")

result = (
    df_orders
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") <= 5)
    .orderBy("status")
)
