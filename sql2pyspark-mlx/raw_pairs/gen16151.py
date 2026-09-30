from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("week_start").orderBy(F.col("price_stddev").desc())

result = (
    df_products
    .groupBy(
        F.date_trunc("WEEK", "launch_date").alias("week_start"),
        F.when(F.col("price") >= 950, "LARGE").otherwise("SMALL").alias("price_tier"),
    )
    .agg(
        F.count(F.when(F.col("category") == "Grocery", 1)).alias("grocery_rows"),
        F.stddev_samp("price").alias("price_stddev"),
        F.round(F.sum("price") / F.nullif(F.count(F.when(F.col("brand") == "Globex", 1)), F.lit(0)), 2).alias("price_per_globex"),
        F.percentile_approx("stock_qty", [0.25, 0.5, 0.75]).alias("stock_qty_quartiles"),
        F.round(F.avg("price"), 2).alias("avg_price"),
        F.percentile_approx("stock_qty", 0.9).alias("stock_qty_p90"),
    )
    .withColumn("price_stddev_rank", F.rank().over(w))
    .orderBy("avg_price")
)
