from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")

result = (
    df_stores
    .withColumn("avg_sqft_by_city", F.avg("sqft").over(w))
    .filter(F.col("sqft") < F.col("avg_sqft_by_city"))
    .select("id", "city", "sqft", "opened_date")
)
