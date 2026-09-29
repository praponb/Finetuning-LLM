from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy(F.col("launch_date").asc_nulls_first(), "id")

result = (
    df_products
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "product_name", "brand", "cost", "launch_date", "supplier_id")
)
