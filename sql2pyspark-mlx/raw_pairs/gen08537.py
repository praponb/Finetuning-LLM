from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("last_restock"), "store_id").orderBy("quantity", "id").rowsBetween(-2, Window.currentRow)
w_part = Window.partitionBy("store_id", "product_id").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part_2 = Window.partitionBy("store_id").orderBy("quantity", "id")

scored = (
    df_inventory
    .select(
        "id",
        "product_id",
        F.avg("quantity").over(w).alias("quantity_avg_2"),
        F.sum("quantity").over(w_part).alias("running_quantity"),
        F.ntile(4).over(w_part_2).alias("quantity_bucket"),
    )
)

result = (
    scored
    .groupBy("product_id")
    .agg(
        F.percentile_approx("quantity_avg_2", 0.9).alias("quantity_avg_2_p90"),
        F.countDistinct("product_id").alias("distinct_product_ids"),
        F.percentile_approx("running_quantity", 0.5).alias("running_quantity_p50"),
        F.round(F.avg("running_quantity"), 2).alias("avg_running_quantity"),
        F.max("quantity_bucket").alias("max_quantity_bucket"),
    )
)
