from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy()

result = (
    df_order_items
    .filter(F.col("unit_price").isNotNull())
    .groupBy(
        F.when(F.col("unit_price") >= 430, "high").when(F.col("unit_price") >= 60, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(
        F.sum("unit_price").alias("total_unit_price"),
        F.count("*").alias("cnt"),
    )
    .withColumn("pct_of_total", F.round(((F.col("total_unit_price") * 100) / F.sum("total_unit_price").over(w)), 2))
    .orderBy("cnt")
)
