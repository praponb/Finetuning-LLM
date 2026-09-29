from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("delivered_date", "id")

result = (
    df_shipments
    .filter(F.col("carrier") == "Kerry")
    .withColumn("rnk", F.row_number().over(w))
    .filter(F.col("rnk") <= 3)
    .select("id", "status", "delivered_date", "carrier")
)
