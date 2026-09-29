from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("sqft", "id")

result = (
    df_stores
    .select("id", "city", "sqft", "region", F.row_number().over(w).alias("rnk"))
    .filter(F.col("rnk") <= 3)
    .select("id", "city", "sqft", "region")
    .orderBy("city")
)
