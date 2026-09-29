from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("store_id", "product_id").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("product_id", "store_id").orderBy("last_restock", "id").rowsBetween(-5, Window.currentRow)
w_recent_2 = Window.partitionBy("product_id").orderBy("last_restock", "id")
w_grp = Window.partitionBy("product_id", "store_id").orderBy("quantity", "id")

with_windows = (
    df_inventory
    .withColumn("cumulative_quantity", F.sum("quantity").over(w_recent))
    .withColumn("quantity_avg_5", F.avg("quantity").over(w2))
    .withColumn("prev_quantity", F.lag("quantity", 1, 0).over(w_recent_2))
    .withColumn("prev_quantity_2", F.lag("quantity", 2).over(w_grp))
    .withColumn("next_quantity", F.lead("quantity", 1).over(w_recent_2))
)

result = (
    with_windows
    .withColumn("quantity_tier", F.when(F.col("quantity") >= 208, "PREMIUM").otherwise("STANDARD"))
    .withColumn("prev_quantity_tier", F.when(F.col("prev_quantity") >= 182, "HIGH").otherwise("LOW"))
    .rollup("quantity_tier", "prev_quantity_tier")
    .agg(
        F.percentile_approx("prev_quantity", [0.25, 0.5, 0.75]).alias("prev_quantity_quartiles"),
        F.percentile_approx("quantity_avg_5", 0.9).alias("quantity_avg_5_p90"),
        F.min("quantity_avg_5").alias("min_quantity_avg_5"),
        F.count("*").alias("num_rows"),
        F.grouping("quantity_tier").alias("grouping_level"),
    )
    .filter(F.col("num_rows") >= 1)
)
