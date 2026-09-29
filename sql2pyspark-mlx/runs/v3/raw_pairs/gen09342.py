from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy()

result = (
    df_products
    .filter(F.col("stock_qty") < 286)
    .groupBy(F.month("launch_date").alias("launch_month"))
    .agg(F.sum("price").alias("total_price"))
    .withColumn("pct_of_total", F.round(((F.col("total_price") * 100) / F.sum("total_price").over(w)), 2))
)
