from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("sqft").desc(), "id")

result = (
    df_stores
    .select("id", "city", "sqft", F.row_number().over(w).alias("rn"))
    .filter(F.col("rn") <= 3)
    .select("id", "city", "sqft")
    .orderBy(F.col("sqft").desc())
)
