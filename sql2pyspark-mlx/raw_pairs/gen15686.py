from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("sale_date").desc(), "id")

deduped = (
    df_store_sales
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

result = (
    deduped
    .select("id", "store_id", "product_id", "units", "revenue")
    .orderBy(F.col("units").asc_nulls_last())
)
