from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy(F.col("delivered_date").desc_nulls_last(), "id")

deduped = (
    df_shipments
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

result = deduped.select("id", "order_id", "shipped_date", "delivered_date", "cost", "status")
