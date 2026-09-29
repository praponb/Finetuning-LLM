from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy()

result = (
    df_order_items
    .groupBy(
        F.when(F.col("quantity") >= 9, "high").when(F.col("quantity") >= 4, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(F.sum("unit_price").alias("total_unit_price"))
    .withColumn("pct_of_total", F.round(((F.col("total_unit_price") * 100) / F.sum("total_unit_price").over(w)), 2))
    .orderBy("quantity_bucket")
)
