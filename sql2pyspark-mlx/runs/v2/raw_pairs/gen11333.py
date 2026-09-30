from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy()

result = (
    df_inventory
    .filter(F.col("product_id").isin([2, 8]))
    .groupBy(F.month("last_restock").alias("last_restock_month"))
    .agg(F.sum("quantity").alias("total_quantity"))
    .withColumn("pct_of_total", F.round(((F.col("total_quantity") * 100) / F.sum("total_quantity").over(w)), 2))
)
