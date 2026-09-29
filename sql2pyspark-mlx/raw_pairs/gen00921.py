from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy("price", "id")

result = (
    df_products
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") <= 3)
)
