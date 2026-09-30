from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy("shipped_date", "id")

valid = (
    df_shipments
    .filter(F.col("shipped_date") >= F.to_date(F.lit("2023-06-07")))
    .withColumn("status_prefix", F.substring("status", 1, 2))
)

result = (
    valid
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)
