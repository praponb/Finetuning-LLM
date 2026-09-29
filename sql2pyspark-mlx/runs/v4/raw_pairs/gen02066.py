from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy(F.col("stock_qty").desc(), "id")
w2 = Window.partitionBy("category").orderBy("launch_date", "id").rowsBetween(-6, Window.currentRow)
w3 = Window.partitionBy("price_avg_6_tier").orderBy(F.col("corr_stock_qty_price").desc())

scored = (
    df_products
    .select(
        "*",
        F.ntile(10).over(w).alias("stock_qty_bucket"),
        F.avg("price").over(w2).alias("price_avg_6"),
    )
)

result = (
    scored
    .groupBy(
        F.when(F.col("price_avg_6") >= 903, "HIGH").otherwise("LOW").alias("price_avg_6_tier"),
        "brand",
    )
    .agg(
        F.avg("price").alias("avg_price"),
        F.round(F.avg("cost"), 2).alias("avg_cost"),
        F.percentile_approx("cost", 0.9).alias("cost_p90"),
        F.corr("stock_qty", "price").alias("corr_stock_qty_price"),
        F.count_if(F.col("category") == "Home").alias("home_events"),
    )
    .filter(F.col("avg_cost") >= 0)
    .withColumn("corr_stock_qty_price_rank", F.rank().over(w3))
)
