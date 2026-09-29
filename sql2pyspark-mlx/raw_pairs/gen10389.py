from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("units_bucket")

result = (
    df_store_sales
    .groupBy(
        F.when(F.col("units") >= 32, "high").when(F.col("units") >= 18, "mid").otherwise("low").alias("units_bucket"),
    )
    .agg(F.sum("units").alias("total_units"))
    .withColumn("cumulative_units", F.sum("total_units").over(w))
    .orderBy(F.col("cumulative_units").desc())
)
