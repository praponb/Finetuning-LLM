from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy()

result = (
    df_inventory
    .groupBy(F.year("last_restock").alias("last_restock_year"))
    .agg(F.sum("quantity").alias("total_quantity"))
    .withColumn("pct_of_total", F.round(((F.col("total_quantity") * 100) / F.sum("total_quantity").over(w)), 2))
    .orderBy(F.col("total_quantity").desc(), "pct_of_total")
)
