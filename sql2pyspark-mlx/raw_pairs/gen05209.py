from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy("cost", "id")

result = (
    df_products
    .filter(F.col("cost").isNull())
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") <= 1)
    .select("id", "brand", "cost", "stock_qty")
    .orderBy("cost")
)
