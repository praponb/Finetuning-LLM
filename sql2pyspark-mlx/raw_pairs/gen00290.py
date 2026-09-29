from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("quantity").desc())

result = (
    df_inventory
    .filter(F.col("quantity") == 194)
    .withColumn("pos", F.dense_rank().over(w))
    .filter(F.col("pos") <= 3)
    .orderBy(F.col("id").desc())
)
