from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy()

result = (
    df_store_sales
    .groupBy(F.month("sale_date").alias("sale_month"))
    .agg(F.sum("units").alias("total_units"))
    .withColumn("pct_of_total", F.round(((F.col("total_units") * 100) / F.sum("total_units").over(w)), 2))
    .orderBy("pct_of_total")
)
