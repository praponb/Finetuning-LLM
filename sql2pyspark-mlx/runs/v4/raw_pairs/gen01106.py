from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("unit_price", "id")

latest = (
    df_order_items
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    latest
    .select("id", "quantity", "unit_price")
    .orderBy(F.col("unit_price").desc_nulls_first())
)
