from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("unit_price").desc())

latest = (
    df_order_items
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
)

result = (
    latest
    .select("id", "order_id", "unit_price")
    .orderBy(F.col("unit_price").desc_nulls_first(), "id")
)
