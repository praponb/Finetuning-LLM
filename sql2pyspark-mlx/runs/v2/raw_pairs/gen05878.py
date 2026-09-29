from pyspark.sql import functions as F

src = df_products.filter(~((F.col("price") >= 770) | (F.col("brand") == "Umbrella")))

summary = (
    src
    .groupBy(
        F.when(F.col("stock_qty") >= 410, "high").when(F.col("stock_qty") >= 139, "mid").otherwise("low").alias("stock_qty_bucket"),
        "brand",
    )
    .agg(F.round(F.avg("price"), 2).alias("avg_price"))
)

result = summary.filter(F.col("avg_price") >= 73)
