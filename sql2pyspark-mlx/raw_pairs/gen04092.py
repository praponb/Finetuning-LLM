from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("unit_price").desc())

deduped = (
    df_order_items
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
)

result = (
    deduped
    .withColumn("unit_price_tier", F.when(F.col("unit_price") >= 400, "LARGE").otherwise("SMALL"))
    .rollup("order_id", "product_id", "unit_price_tier")
    .agg(
        F.avg("quantity").alias("avg_quantity"),
        F.sum("unit_price").alias("total_unit_price"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity_2"),
        F.max("quantity").alias("max_quantity"),
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
        F.stddev_samp("quantity").alias("quantity_stddev"),
        F.grouping("order_id").alias("grouping_level"),
    )
    .filter(F.col("quantity_stddev") < 1000000)
    .orderBy(F.col("unit_price_tier").desc())
)
