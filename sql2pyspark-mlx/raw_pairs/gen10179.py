from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy("stock_qty", "id")

result = (
    df_products
    .withColumn("pos", F.row_number().over(w))
    .filter(F.col("pos") <= 1)
)
