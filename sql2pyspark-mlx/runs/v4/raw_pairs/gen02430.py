from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("delivered_date").desc(), "id")

result = (
    df_shipments
    .filter(F.col("carrier") == "FedEx")
    .withColumn("rnk", F.row_number().over(w))
    .filter(F.col("rnk") <= 2)
    .orderBy("rnk")
)
