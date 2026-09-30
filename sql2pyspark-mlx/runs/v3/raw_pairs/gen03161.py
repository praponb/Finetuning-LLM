from pyspark.sql import functions as F

base = df_products.filter(F.array_contains("tags", "clearance") & (F.col("cost") > 710))

agg = (
    base
    .groupBy("brand", "category")
    .agg(
        F.round(F.avg("stock_qty"), 2).alias("avg_stock_qty"),
        (F.max("cost") - F.min("cost")).alias("cost_range"),
    )
)

result = agg.filter(F.col("avg_stock_qty") > 22)
