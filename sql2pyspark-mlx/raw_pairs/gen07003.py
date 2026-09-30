from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "carrier").orderBy(F.col("shipped_date").asc_nulls_first())

deduped = (
    df_shipments
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
)

result = (
    deduped
    .select("id", "carrier", "delivered_date", "status")
    .orderBy("delivered_date")
)
