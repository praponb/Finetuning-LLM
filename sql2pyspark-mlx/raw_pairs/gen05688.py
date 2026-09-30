from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("carrier_length").desc(), "id")

filtered = (
    df_shipments
    .filter(F.col("carrier").isNotNull())
    .select("*", F.length("carrier").alias("carrier_length"))
)

result = (
    filtered
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)
