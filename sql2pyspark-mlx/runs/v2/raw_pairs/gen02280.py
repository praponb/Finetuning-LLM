from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("sale_date").desc(), "id")

result = (
    df_store_sales
    .filter(F.col("product_id").isin([2, 3, 4]))
    .withColumn("pos", F.row_number().over(w))
    .filter(F.col("pos") == 1)
    .select("id", "store_id", "sale_date")
    .orderBy(F.col("store_id").desc())
)
