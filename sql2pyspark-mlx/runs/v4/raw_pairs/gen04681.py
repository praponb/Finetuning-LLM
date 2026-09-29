from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy()

result = (
    df_store_sales
    .groupBy(F.year("sale_date").alias("sale_year"))
    .agg(F.sum("revenue").alias("total_revenue"))
    .withColumn("pct_of_total", F.round(((F.col("total_revenue") * 100) / F.sum("total_revenue").over(w)), 2))
)
