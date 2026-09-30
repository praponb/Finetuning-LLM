from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy(F.col("cost").desc_nulls_first())

latest = (
    df_products
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

result = (
    latest
    .groupBy(
        F.when(F.col("stock_qty") >= 444, "HIGH").otherwise("LOW").alias("stock_qty_tier"),
    )
    .agg(
        F.count(F.when(F.col("brand") == "Umbrella", 1)).alias("umbrella_rows"),
        F.round(F.avg("stock_qty"), 2).alias("avg_stock_qty"),
        F.bit_or("stock_qty").alias("stock_qty_bit_or"),
    )
    .filter(F.col("umbrella_rows") > 1)
    .orderBy(F.col("avg_stock_qty").desc_nulls_first())
)
