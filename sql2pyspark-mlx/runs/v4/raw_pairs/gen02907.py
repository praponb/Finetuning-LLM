from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy("launch_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("brand").orderBy("cost", "id")

metrics = (
    df_products
    .withColumn("first_known_brand", F.first("brand", ignorenulls=True).over(w))
    .withColumn("prev_cost", F.lag("cost", 1, 0).over(w2))
)

result = (
    metrics
    .rollup("brand", F.date_trunc("WEEK", "launch_date").alias("week_start"))
    .agg(
        F.avg(F.when(F.col("brand") == "Stark", F.col("price"))).alias("stark_avg_price"),
        F.round(F.avg("stock_qty"), 2).alias("avg_stock_qty"),
    )
    .orderBy(F.col("week_start").asc_nulls_last())
)
