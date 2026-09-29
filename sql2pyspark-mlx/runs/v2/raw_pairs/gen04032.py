from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy()

result = (
    df_stores
    .filter(F.col("city") == "Bangkok")
    .groupBy(F.date_trunc("MONTH", "opened_date").alias("opened_month"))
    .agg(F.sum("sqft").alias("total_sqft"))
    .withColumn("pct_of_total", F.round(((F.col("total_sqft") * 100) / F.sum("total_sqft").over(w)), 2))
    .orderBy(F.col("total_sqft").desc(), F.col("opened_month").desc())
)
