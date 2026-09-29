from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy()

result = (
    df_stores
    .filter(F.col("region").isin(["Central", "North", "East"]))
    .groupBy(
        F.when(F.col("city").isin(["Bangkok", "Tokyo"]), "core").otherwise("other").alias("city_group"),
    )
    .agg(F.sum("sqft").alias("total_sqft"))
    .withColumn("pct_of_total", F.round(((F.col("total_sqft") * 100) / F.sum("total_sqft").over(w)), 2))
    .orderBy("pct_of_total")
)
