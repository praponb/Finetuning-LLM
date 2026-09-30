from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id")

result = (
    df_store_sales
    .withColumn("avg_units_by_store", F.avg("units").over(w))
    .filter(F.col("units") > F.col("avg_units_by_store"))
    .select("id", "store_id", "units")
)
