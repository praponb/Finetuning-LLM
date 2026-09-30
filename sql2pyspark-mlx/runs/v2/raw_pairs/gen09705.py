from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy()

result = (
    df_departments
    .filter(F.col("location").isin(["Chiang Mai", "Berlin"]))
    .groupBy("location")
    .agg(F.sum("budget").alias("total_budget"))
    .withColumn("pct_of_total", F.round(((F.col("total_budget") * 100) / F.sum("total_budget").over(w)), 2))
)
