from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("amount").desc(), "id")

result = (
    df_orders
    .filter(F.col("amount") > 640)
    .withColumn("rnk", F.row_number().over(w))
    .filter(F.col("rnk") <= 2)
    .select("id", "status", "amount")
    .orderBy(F.col("id").desc(), F.col("amount").desc())
)
