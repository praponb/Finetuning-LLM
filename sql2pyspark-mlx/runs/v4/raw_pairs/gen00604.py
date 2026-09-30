from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id")

result = (
    df_store_sales
    .withColumn("min_units_by_product", F.min("units").over(w))
    .filter(F.col("units") == F.col("min_units_by_product"))
    .select("id", "product_id", "units")
    .orderBy(F.col("id").desc(), F.col("units").desc())
)
