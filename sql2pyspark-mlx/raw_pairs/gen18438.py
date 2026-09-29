from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("unit_price", "id").rowsBetween(-5, -1)
w2 = Window.partitionBy("product_id").orderBy("line_total", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("product_id").orderBy("line_total_2", "id")
w_grp = Window.partitionBy("product_id").orderBy("quantity_avg_5_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .withColumn("line_total", (F.col("unit_price") + F.col("quantity")))
    .withColumn("line_total_2", (F.col("quantity") * F.col("unit_price")))
)

with_windows = (
    valid
    .select(
        "id",
        "order_id",
        "product_id",
        "line_total_2",
        F.avg("quantity").over(w).alias("quantity_avg_5_prior"),
        F.first("unit_price", ignorenulls=True).over(w2).alias("first_known_unit_price"),
        F.lag("quantity", 1).over(w3).alias("prev_quantity"),
    )
)

valid_order_items = (
    with_windows
    .filter(F.col("prev_quantity") <= 7)
    .withColumn("quantity_avg_5_prior_rounded", F.round("quantity_avg_5_prior", 0))
)

metrics = (
    valid_order_items
    .select(
        "id",
        "line_total_2",
        "first_known_unit_price",
        "prev_quantity",
        "quantity_avg_5_prior_rounded",
        F.avg("line_total_2").over(w_grp).alias("running_avg_line_total_2"),
        F.min("line_total_2").over(w_grp).alias("running_min_line_total_2"),
        F.count("line_total_2").over(w_grp).alias("running_count_line_total_2"),
    )
)

result = (
    metrics
    .select(
        "id",
        "first_known_unit_price",
        "running_avg_line_total_2",
        "running_min_line_total_2",
    )
)
