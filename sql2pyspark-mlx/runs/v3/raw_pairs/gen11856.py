from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("quantity").desc(), "id")

result = (
    df_inventory
    .filter(F.col("product_id").isin([3, 6]))
    .withColumn("pos", F.row_number().over(w))
    .filter(F.col("pos") <= 1)
    .orderBy(F.col("product_id").desc())
)
