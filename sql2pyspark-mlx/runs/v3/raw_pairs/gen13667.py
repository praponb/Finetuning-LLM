from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("cost", "id")

result = (
    df_shipments
    .filter(F.col("shipped_date") > "2025-05-01")
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "status", "cost", "carrier")
)
