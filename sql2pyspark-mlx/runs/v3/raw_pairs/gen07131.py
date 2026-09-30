from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy("launch_date")

result = (
    df_products
    .filter(F.col("cost").isNull())
    .withColumn("pos", F.rank().over(w))
    .filter(F.col("pos") <= 1)
    .orderBy("pos")
)
