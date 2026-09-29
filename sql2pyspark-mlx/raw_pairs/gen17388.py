from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id")
w2 = Window.partitionBy("product_id").orderBy(F.col("units").desc(), "id")
w3 = Window.partitionBy("product_id").orderBy("units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("store_id", "product_id").orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_windows = (
    df_store_sales
    .filter(F.col("revenue") >= 4000)
    .withColumn("units_share", F.round((F.col("units") / F.sum("units").over(w)), 4))
    .withColumn("units_bucket", F.ntile(10).over(w2))
    .withColumn("first_known_units", F.first("units", ignorenulls=True).over(w3))
    .withColumn("running_units", F.sum("units").over(w4))
)

result = (
    with_windows
    .groupBy("store_id")
    .agg(
        F.percentile_approx("units", [0.25, 0.5, 0.75]).alias("units_quartiles"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("units"), 2).alias("avg_units"),
    )
    .orderBy(F.col("avg_units").asc_nulls_last())
)
