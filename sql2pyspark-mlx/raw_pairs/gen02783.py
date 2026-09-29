from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy()

result = (
    df_inventory
    .filter(F.col("quantity") > 236)
    .groupBy("store_id")
    .agg(F.sum("quantity").alias("total_quantity"))
    .withColumn("pct_of_total", F.round(((F.col("total_quantity") * 100) / F.sum("total_quantity").over(w)), 2))
    .orderBy("total_quantity", "pct_of_total", "store_id")
    .limit(10)
)
