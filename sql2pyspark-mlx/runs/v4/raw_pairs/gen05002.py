from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("quantity").desc(), "id")

latest = (
    df_inventory
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

result = (
    latest
    .select("id", "store_id", "quantity")
    .orderBy(F.col("quantity").asc_nulls_first(), "store_id")
)
