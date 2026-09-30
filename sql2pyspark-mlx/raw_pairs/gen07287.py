from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())

with_last = (
    df_products
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.quantity").alias("inventory_quantity"),
        F.col("c.last_restock").alias("inventory_last_restock"),
    )
)

result = (
    with_last
    .groupBy("supplier_id")
    .agg(
        F.sort_array(F.collect_set("brand")).alias("brand_list"),
        F.count(F.when(F.col("category") == "Fashion", 1)).alias("fashion_rows"),
        F.round(F.avg("inventory_quantity"), 2).alias("avg_inventory_quantity"),
        F.stddev_samp("cost").alias("cost_stddev"),
        F.count_if(F.col("category") == "Sports").alias("sports_events"),
    )
    .filter(F.col("fashion_rows") > 1)
)
