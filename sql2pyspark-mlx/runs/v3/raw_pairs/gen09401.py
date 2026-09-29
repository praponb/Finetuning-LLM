from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("delivered_date").desc(), "id")

result = (
    df_shipments
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "status", "delivered_date", "cost")
    .orderBy(F.col("status").desc(), F.col("id").desc())
)
