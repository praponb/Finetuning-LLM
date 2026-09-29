from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy(F.col("price").desc())

result = (
    df_products
    .filter(F.col("supplier_id").isNull())
    .withColumn("pos", F.dense_rank().over(w))
    .filter(F.col("pos") == 1)
    .select("id", "supplier_id", "price")
)
