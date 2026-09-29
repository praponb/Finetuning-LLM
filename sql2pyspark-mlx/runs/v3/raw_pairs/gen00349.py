from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id")

result = (
    df_store_sales
    .withColumn("min_units_by_store", F.min("units").over(w))
    .filter(F.col("units") == F.col("min_units_by_store"))
    .select("id", "store_id", "units")
    .orderBy("units", F.col("id").desc())
)
