from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy()

result = (
    df_store_sales
    .groupBy(
        F.when(F.col("units") >= 38, "high").when(F.col("units") >= 11, "mid").otherwise("low").alias("units_bucket"),
    )
    .agg(F.sum("units").alias("total_units"))
    .withColumn("pct_of_total", F.round(((F.col("total_units") * 100) / F.sum("total_units").over(w)), 2))
)
