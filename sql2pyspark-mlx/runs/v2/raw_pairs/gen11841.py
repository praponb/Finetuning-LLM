from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("quantity_bucket")

result = (
    df_order_items
    .groupBy(
        F.when(F.col("quantity") >= 7, "high").when(F.col("quantity") >= 2, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(F.sum("unit_price").alias("total_unit_price"))
    .withColumn("cumulative_unit_price", F.sum("total_unit_price").over(w))
    .orderBy(F.col("cumulative_unit_price").desc())
)
