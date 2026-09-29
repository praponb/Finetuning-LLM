from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("revenue").desc(), "id")

result = (
    df_store_sales
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") <= 2)
    .select("id", "product_id", "revenue", "sale_date")
    .orderBy(F.col("product_id").desc())
)
