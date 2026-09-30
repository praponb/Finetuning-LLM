from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy(F.col("delivered_date").desc_nulls_first())

deduped = (
    df_shipments
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

result = deduped.select("id", "carrier", "shipped_date", "delivered_date", "cost", "status")
