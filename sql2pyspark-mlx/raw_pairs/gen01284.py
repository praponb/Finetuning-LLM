from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "status").orderBy(F.col("shipped_date").desc(), "id")

latest = (
    df_shipments
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

result = latest.select("id", "order_id", "carrier", "shipped_date", "delivered_date", "cost")
