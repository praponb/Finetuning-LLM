from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("quantity").desc(), "id")

result = (
    df_inventory
    .filter(F.col("store_id").isin([2, 3, 4, 5]))
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") <= 1)
)
