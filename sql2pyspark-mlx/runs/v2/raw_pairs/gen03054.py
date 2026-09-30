from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy(F.col("cost").desc())

result = (
    df_products
    .withColumn("rnk", F.rank().over(w))
    .filter(F.col("rnk") <= 3)
)
