from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy(F.col("launch_date").desc(), "id")

result = (
    df_products
    .filter(F.col("stock_qty").between(267, 353))
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") <= 5)
    .orderBy(F.col("id").desc(), F.col("cost").desc())
)
