from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("units").desc())
w2 = Window.partitionBy("store_id").orderBy("units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("store_id").orderBy("sale_date_day", "id")

filtered = (
    df_store_sales
    .filter(F.col("sale_date") > F.lit("2023-03-21").cast("date"))
    .withColumn("sale_date_day", F.dayofmonth("sale_date"))
)

result = (
    filtered
    .withColumn("units_percent_rank", F.percent_rank().over(w))
    .withColumn("cumulative_sale_date_day", F.sum("sale_date_day").over(w2))
    .withColumn("prev_sale_date_day", F.lag("sale_date_day", 1).over(w3))
)
