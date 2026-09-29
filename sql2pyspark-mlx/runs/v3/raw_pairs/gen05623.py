from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("sqft").desc(), "id")

result = (
    df_stores
    .select("id", "city", "sqft", F.row_number().over(w).alias("pos"))
    .filter(F.col("pos") <= 1)
    .orderBy("pos")
)
