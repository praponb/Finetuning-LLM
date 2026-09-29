from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("sqft", "id")

result = (
    df_stores
    .filter(F.month("opened_date").isin([8, 9, 10]))
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") <= 2)
    .select("id", "region", "sqft", "city")
    .orderBy("region", "city")
)
