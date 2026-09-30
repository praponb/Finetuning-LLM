from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("sale_date", "id").rowsBetween(-5, Window.currentRow)
w_hist = Window.partitionBy("product_id").orderBy("revenue", "id")
w3 = Window.partitionBy("product_id", "store_id").orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_store_sales
    .filter(F.col("sale_date") > F.lit("2023-03-19").cast("date"))
    .select(
        "*",
        F.when(F.col("units") >= 45, "High").when(F.col("units") >= 15, "Medium").otherwise("Low").alias("units_tier"),
        F.least("units", "revenue").alias("least_units_revenue"),
    )
)

result = (
    base
    .withColumn("units_avg_5", F.avg("units").over(w))
    .withColumn("seq_no", F.row_number().over(w_hist))
    .withColumn("cumulative_revenue", F.sum("revenue").over(w3))
)
