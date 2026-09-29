from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy(F.col("launch_date").desc(), "id")
w2 = Window.orderBy("cost", "id")
w_grp = Window.partitionBy("brand")
w_hist = Window.partitionBy("category")
w5 = Window.partitionBy("category").orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_products
    .filter((F.col("launch_date") >= F.lit("2024-03-27").cast("date")) & F.col("brand").isNotNull())
    .withColumn("price_level", F.when(F.col("price") >= 1300, "High").when(F.col("price") >= 320, "Medium").otherwise("Low"))
    .withColumn("sorted_tags", F.sort_array("tags"))
)

result = (
    valid
    .withColumn("event_seq", F.row_number().over(w))
    .withColumn("cost_bucket", F.ntile(5).over(w2))
    .withColumn("brand_min_cost", F.min("cost").over(w_grp))
    .withColumn("category_stddev_samp_stock_qty", F.stddev_samp("stock_qty").over(w_hist))
    .withColumn("last_known_price", F.last("price", ignorenulls=True).over(w5))
)
