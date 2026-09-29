from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy()

result = (
    df_order_items
    .filter(F.col("order_id").isin([3, 20]))
    .groupBy("product_id")
    .agg(
        F.sum("unit_price").alias("total_unit_price"),
        F.count("*").alias("cnt"),
    )
    .withColumn("pct_of_total", F.round(((F.col("total_unit_price") * 100) / F.sum("total_unit_price").over(w)), 2))
    .orderBy(F.col("total_unit_price").desc(), "product_id")
)
