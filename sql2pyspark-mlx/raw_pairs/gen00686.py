from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("product_id", "store_id").orderBy("sale_date", "id").rowsBetween(-2, Window.currentRow)

base = (
    df_store_sales
    .filter(F.col("units") > 36)
    .withColumn("revenue_level", F.when(F.col("revenue") >= 3900, "High").when(F.col("revenue") >= 1700, "Medium").otherwise("Low"))
    .withColumn("sale_date_label", F.date_format("sale_date", "MMM yyyy"))
)

result = (
    base
    .filter(F.col("product_id").isin([2, 8, 9]))
    .withColumn("last3_avg_units", F.avg("units").over(w_ordered))
    .withColumn("last3_max_units", F.max("units").over(w_ordered))
    .withColumn("last3_count_units", F.count("units").over(w_ordered))
    .withColumn("last3_min_units", F.min("units").over(w_ordered))
)
