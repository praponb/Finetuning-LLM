from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("quantity").desc_nulls_last(), "id")
w2 = Window.partitionBy("order_id").orderBy("quantity", "id").rowsBetween(-2, Window.currentRow)
w3 = Window.orderBy("quantity", "id")
w_recent = Window.partitionBy("order_id").orderBy("last3_sum_quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.orderBy("adjusted_prev_quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

ranked = (
    df_order_items
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "order_id", "product_id", "quantity")
)

scored = (
    ranked
    .select(
        "*",
        F.min("quantity").over(w2).alias("last3_min_quantity"),
        F.sum("quantity").over(w2).alias("last3_sum_quantity"),
        F.avg("quantity").over(w2).alias("last3_avg_quantity"),
        F.max("quantity").over(w2).alias("last3_max_quantity"),
        F.lag("quantity", 1, 0).over(w3).alias("prev_quantity"),
    )
)

metrics = (
    scored
    .filter(F.col("last3_max_quantity") <= 6.2)
    .select(
        "id",
        "prev_quantity",
        F.count("last3_min_quantity").over(w_recent).alias("running_count_last3_min_quantity"),
        F.avg("last3_min_quantity").over(w_recent).alias("running_avg_last3_min_quantity"),
        F.sum("last3_min_quantity").over(w_recent).alias("running_sum_last3_min_quantity"),
    )
)

cleaned = (
    metrics
    .filter(F.col("prev_quantity") <= 8)
    .select(
        "id",
        "running_avg_last3_min_quantity",
        F.col("running_sum_last3_min_quantity").cast("int").alias("running_sum_last3_min_quantity_int"),
        F.round((F.col("prev_quantity") * 100), 2).alias("adjusted_prev_quantity"),
    )
)

result = (
    cleaned
    .withColumn("running_avg_running_sum_last3_min_quantity_int", F.avg("running_sum_last3_min_quantity_int").over(w_grp))
    .withColumn("running_sum_running_sum_last3_min_quantity_int", F.sum("running_sum_last3_min_quantity_int").over(w_grp))
    .withColumn("running_min_running_sum_last3_min_quantity_int", F.min("running_sum_last3_min_quantity_int").over(w_grp))
)
