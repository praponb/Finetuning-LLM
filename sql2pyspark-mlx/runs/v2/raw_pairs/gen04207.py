from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy(F.col("sqft").desc(), "id")

result = (
    df_stores
    .select("id", "region", "sqft", "city", F.row_number().over(w).alias("pos"))
    .filter(F.col("pos") <= 3)
)
