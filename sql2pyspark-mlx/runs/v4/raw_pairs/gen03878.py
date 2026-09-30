from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy(F.col("launch_date").desc())

result = (
    df_products
    .withColumn("pos", F.dense_rank().over(w))
    .filter(F.col("pos") <= 3)
    .orderBy("id")
)
