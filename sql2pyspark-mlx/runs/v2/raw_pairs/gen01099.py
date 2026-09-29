from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy(F.col("stock_qty").desc(), "id")

result = (
    df_products
    .filter(F.col("cost") > 280)
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") <= 3)
    .orderBy(F.col("brand").desc())
)
