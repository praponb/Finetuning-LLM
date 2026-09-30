from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("sqft", "id")

result = (
    df_stores
    .filter(F.col("store_name").isNull())
    .select("id", "region", "sqft", F.row_number().over(w).alias("row_num"))
    .filter(F.col("row_num") <= 1)
    .select("id", "region", "sqft")
)
