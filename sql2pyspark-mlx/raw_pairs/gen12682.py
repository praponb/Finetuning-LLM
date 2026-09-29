from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("sqft", "id")

result = (
    df_stores
    .filter(F.col("city") == "Tokyo")
    .select("id", "region", "sqft", "store_name", F.row_number().over(w).alias("pos"))
    .filter(F.col("pos") <= 3)
    .select("id", "region", "sqft", "store_name")
    .orderBy("id", "sqft")
)
