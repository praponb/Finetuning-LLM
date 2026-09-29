from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy(F.col("sqft").desc(), "id")

result = (
    df_stores
    .filter(F.col("region").isin(["West", "South", "Central"]))
    .select("id", "region", "sqft", "city", F.first("city").over(w).alias("first_city"))
    .orderBy(F.col("city").desc(), "region")
)
