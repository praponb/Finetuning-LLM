from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id")

result = (
    df_store_sales
    .withColumn("avg_units_by_product", F.avg("units").over(w))
    .filter(F.col("units") < F.col("avg_units_by_product"))
    .select("id", "product_id", "units", "store_id")
)
