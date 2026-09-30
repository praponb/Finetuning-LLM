from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy("cost", "id")

result = (
    df_shipments
    .filter(F.col("status") != "DELIVERED")
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") <= 3)
    .select("id", "carrier", "cost")
    .orderBy("id", F.col("carrier").desc())
)
