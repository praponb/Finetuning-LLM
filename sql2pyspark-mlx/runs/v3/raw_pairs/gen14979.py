from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy()

result = (
    df_products
    .filter(F.array_contains("tags", "eco"))
    .groupBy("brand")
    .agg(F.sum("price").alias("total_price"))
    .withColumn("pct_of_total", F.round(((F.col("total_price") * 100) / F.sum("total_price").over(w)), 2))
)
