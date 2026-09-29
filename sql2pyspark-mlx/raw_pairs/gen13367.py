from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy(F.col("launch_date").desc(), "id")

result = (
    df_products
    .filter(F.col("launch_date") > "2023-08-01")
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "category", "launch_date", "product_name")
    .orderBy("product_name", F.col("launch_date").desc())
)
