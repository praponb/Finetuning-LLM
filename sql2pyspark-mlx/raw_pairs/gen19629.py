from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("quantity").asc_nulls_last(), "id")

cleaned = (
    df_inventory
    .filter(F.col("last_restock") >= F.lit("2023-02-16").cast("date"))
    .select("*", F.floor("quantity").alias("quantity_whole"))
)

result = (
    cleaned
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "product_id")
)
