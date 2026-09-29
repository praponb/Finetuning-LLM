from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy()

result = (
    df_order_items
    .groupBy(
        F.when(F.col("unit_price") >= 380, "high").when(F.col("unit_price") >= 80, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.count("*").alias("cnt"),
    )
    .withColumn("pct_of_total", F.round(((F.col("total_quantity") * 100) / F.sum("total_quantity").over(w)), 2))
    .orderBy("total_quantity")
)
