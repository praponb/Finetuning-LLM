from pyspark.sql import functions as F

base = (
    df_products
    .filter(F.array_contains("tags", "eco") | F.array_contains("tags", "premium"))
    .withColumn("days_since_launch_date", F.datediff(F.current_date(), "launch_date"))
)

agg = (
    base
    .groupBy(
        F.when(F.col("stock_qty") >= 407, "high").when(F.col("stock_qty") >= 101, "mid").otherwise("low").alias("stock_qty_bucket"),
        "category",
    )
    .agg(
        F.round(F.stddev("days_since_launch_date"), 2).alias("stddev_days_since_launch_date"),
    )
)

result = (
    agg
    .select("stock_qty_bucket", "category", "stddev_days_since_launch_date")
    .orderBy("stock_qty_bucket", "stddev_days_since_launch_date")
)
