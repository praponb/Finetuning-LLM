from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_quantity").desc())

result = (
    df_inventory
    .filter(F.col("last_restock").between("2023-05-04", "2023-08-01"))
    .groupBy(F.date_format("last_restock", "yyyy-MM").alias("last_restock_month"))
    .agg(F.sum("quantity").alias("total_quantity"))
    .withColumn("quantity_rank", F.dense_rank().over(w))
)
