from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("order_id")

result = (
    df_order_items
    .filter(F.col("quantity") < 8)
    .groupBy("order_id")
    .agg(
        F.sum("unit_price").alias("total_unit_price"),
        F.count("*").alias("cnt"),
    )
    .withColumn("cumulative_unit_price", F.sum("total_unit_price").over(w))
    .orderBy(F.col("cnt").desc())
)
