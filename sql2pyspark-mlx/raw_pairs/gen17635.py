from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy(F.year("last_restock"), "product_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.month("last_restock"), "product_id").orderBy(F.col("running_avg_quantity").desc(), "id")
w_ordered = Window.partitionBy("store_id").orderBy("running_quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy(F.month("last_restock"), "product_id").orderBy("running_quantity", "id")
w6 = Window.partitionBy("product_id").orderBy("running_count_quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("store_id").orderBy("running_avg_quantity", "id").rowsBetween(-2, -1)
w8 = Window.partitionBy(F.date_trunc("WEEK", "last_restock"), "store_id").orderBy("cumulative_running_min_quantity", "id").rowsBetween(-3, Window.currentRow)
w9 = Window.partitionBy("city").orderBy(F.col("last_restock").desc(), "id")
w10 = Window.partitionBy("city", "store_id").orderBy("running_count_quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("store_id", "city").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("store_id").orderBy("opened_date", "id").rowsBetween(-3, Window.currentRow)

scored = (
    df_inventory
    .select(
        "id",
        "store_id",
        "product_id",
        "last_restock",
        F.avg("quantity").over(w).alias("running_avg_quantity"),
        F.max("quantity").over(w).alias("running_max_quantity"),
        F.count("quantity").over(w).alias("running_count_quantity"),
        F.min("quantity").over(w).alias("running_min_quantity"),
        F.sum("quantity").over(w2).alias("running_quantity"),
    )
)

metrics = (
    scored
    .filter(F.col("store_id").isin([2, 4]))
    .select(
        "*",
        F.row_number().over(w3).alias("row_seq"),
        F.sum("running_max_quantity").over(w_ordered).alias("cumulative_running_max_quantity"),
        F.ntile(5).over(w_hist).alias("running_quantity_bucket"),
        F.sum("running_min_quantity").over(w6).alias("cumulative_running_min_quantity"),
        F.avg("running_max_quantity").over(w7).alias("running_max_quantity_avg_2_prior"),
    )
)

with_dims = (
    metrics
    .alias("e")
    .join(df_stores.alias("b"), F.col("e.store_id") == F.col("b.id"), "left")
    .select(
        "e.id",
        "e.store_id",
        "e.last_restock",
        "e.running_avg_quantity",
        "e.running_count_quantity",
        "e.row_seq",
        "e.cumulative_running_min_quantity",
        F.col("b.city"),
    )
)

metrics_inventory = (
    with_dims
    .select(
        "id",
        "store_id",
        "running_avg_quantity",
        "row_seq",
        "cumulative_running_min_quantity",
        "city",
        F.avg("row_seq").over(w8).alias("row_seq_avg_3"),
        F.row_number().over(w9).alias("event_seq"),
        F.sum("running_count_quantity").over(w10).alias("cumulative_running_count_quantity"),
    )
)

joined = (
    metrics_inventory
    .alias("src")
    .join(df_stores.alias("s"), F.col("src.store_id") == F.col("s.id"), "left")
    .select(
        "src.id",
        "src.store_id",
        "src.cumulative_running_min_quantity",
        "src.city",
        "src.row_seq_avg_3",
        "src.cumulative_running_count_quantity",
        F.col("s.opened_date"),
    )
)

result = (
    joined
    .select(
        "id",
        "store_id",
        "cumulative_running_min_quantity",
        "city",
        "row_seq_avg_3",
        "cumulative_running_count_quantity",
        F.count("cumulative_running_count_quantity").over(w_part).alias("running_count_cumulative_running_count_quantity"),
        F.sum("cumulative_running_count_quantity").over(w_part).alias("running_sum_cumulative_running_count_quantity"),
        F.avg("cumulative_running_count_quantity").over(w_part).alias("running_avg_cumulative_running_count_quantity"),
        F.avg("cumulative_running_min_quantity").over(w_grp).alias("cumulative_running_min_quantity_avg_3"),
    )
)
