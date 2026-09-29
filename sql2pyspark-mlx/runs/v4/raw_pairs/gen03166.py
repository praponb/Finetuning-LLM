from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("quantity").asc_nulls_first())

base = (
    df_order_items
    .filter(F.col("product_id").isin([21, 33]))
    .select("*", (F.col("quantity") + F.col("unit_price")).alias("line_total"))
)

result = (
    base
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)
