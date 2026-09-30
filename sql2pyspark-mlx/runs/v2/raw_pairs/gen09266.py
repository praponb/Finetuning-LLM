from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("opened_year")

result = (
    df_stores
    .groupBy(F.year("opened_date").alias("opened_year"))
    .agg(F.sum("sqft").alias("total_sqft"))
    .withColumn("cumulative_sqft", F.sum("total_sqft").over(w))
)
