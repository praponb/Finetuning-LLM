from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy(F.col("cost").desc(), "id")

result = (
    df_products
    .filter(F.col("cost") > 380)
    .withColumn("pos", F.row_number().over(w))
    .filter(F.col("pos") <= 2)
    .orderBy(F.col("pos").desc())
)
