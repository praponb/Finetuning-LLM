from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("sqft").desc(), "id")

result = (
    df_stores
    .select("id", "city", "sqft", F.ntile(4).over(w).alias("sqft_ntile"))
    .orderBy(F.col("city").desc(), F.col("sqft_ntile").desc())
)
