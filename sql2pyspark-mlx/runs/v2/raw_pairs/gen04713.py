from pyspark.sql import functions as F

src = df_inventory.filter(F.col("last_restock") >= "2024-09-01")

agg = (
    src
    .groupBy(
        F.when(F.col("quantity") >= 247, "high").when(F.col("quantity") >= 57, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(F.round(F.stddev("quantity"), 2).alias("stddev_quantity"))
)

result = (
    agg
    .select("quantity_bucket", "stddev_quantity")
    .orderBy("quantity_bucket", F.col("stddev_quantity").desc())
)
