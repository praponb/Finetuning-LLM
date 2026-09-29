from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("last_restock"), "product_id").orderBy(F.col("quantity").desc(), "id")
w2 = Window.partitionBy("product_id").orderBy("quantity", "id")

with_windows = (
    df_inventory
    .withColumn("row_seq", F.row_number().over(w))
    .withColumn("prev_quantity", F.lag("quantity", 1).over(w2))
)

result = (
    with_windows
    .groupBy("product_id", "store_id")
    .agg(
        F.countDistinct("store_id").alias("distinct_store_ids"),
        F.countDistinct("product_id").alias("distinct_product_ids"),
        F.count("*").alias("cnt"),
        F.percentile_approx("quantity", [0.25, 0.5, 0.75]).alias("quantity_quartiles"),
    )
    .orderBy(F.col("cnt").asc_nulls_last())
)
