from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("quantity", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("store_id").orderBy("quantity", "id").rowsBetween(-3, -1)
w3 = Window.partitionBy("product_id").orderBy("opened_date", "id")
w4 = Window.partitionBy(F.month("opened_date"), "store_id").orderBy("last_restock", "id")
w5 = Window.partitionBy("product_id").orderBy("last7_avg_quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy(F.month("opened_date")).orderBy(F.col("last7_min_quantity").desc(), "id")
w8 = Window.partitionBy(F.date_trunc("month", "last_restock"), "store_id").orderBy("quantity_avg_3_prior", "id").rowsBetween(-3, Window.currentRow)
w9 = Window.partitionBy("product_id").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.orderBy("quantity_avg_3_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w11 = Window.partitionBy(F.year("last_restock"), "store_id").orderBy("next_last7_avg_quantity", "id")
w12 = Window.partitionBy(F.date_trunc("month", "opened_date"), "store_id").orderBy(F.col("running_last7_min_quantity").desc_nulls_last())
w13 = Window.orderBy("running_last7_min_quantity", "id")
w14 = Window.partitionBy("store_id").orderBy(F.col("last_restock").desc(), "id")

enriched = (
    df_inventory
    .select(
        "id",
        "store_id",
        "product_id",
        "last_restock",
        F.avg("quantity").over(w).alias("last7_avg_quantity"),
        F.min("quantity").over(w).alias("last7_min_quantity"),
        F.max("quantity").over(w).alias("last7_max_quantity"),
        F.sum("quantity").over(w).alias("last7_sum_quantity"),
        F.avg("quantity").over(w2).alias("quantity_avg_3_prior"),
    )
)

enriched_inventory = (
    enriched
    .alias("s")
    .join(df_stores.alias("b"), F.col("s.store_id") == F.col("b.id"), "inner")
    .select(
        "s.id",
        "s.store_id",
        "s.product_id",
        "s.last_restock",
        "s.last7_avg_quantity",
        "s.last7_min_quantity",
        "s.quantity_avg_3_prior",
        F.col("b.opened_date"),
        F.col("b.store_name"),
    )
)

metrics = (
    enriched_inventory
    .select(
        "*",
        F.lag("last7_min_quantity", 1).over(w3).alias("prev_last7_min_quantity"),
        F.lead("last7_avg_quantity", 1, 0).over(w4).alias("next_last7_avg_quantity"),
        F.sum("last7_avg_quantity").over(w5).alias("cumulative_last7_avg_quantity"),
        F.sum("last7_min_quantity").over(w6).alias("running_last7_min_quantity"),
        F.ntile(5).over(w7).alias("last7_min_quantity_bucket"),
    )
)

metrics_inventory = (
    metrics
    .select(
        "id",
        "store_id",
        "last_restock",
        "last7_avg_quantity",
        "quantity_avg_3_prior",
        "opened_date",
        "next_last7_avg_quantity",
        "cumulative_last7_avg_quantity",
        "running_last7_min_quantity",
        F.avg("cumulative_last7_avg_quantity").over(w8).alias("cumulative_last7_avg_quantity_avg_3"),
        F.sum("quantity_avg_3_prior").over(w9).alias("cumulative_quantity_avg_3_prior"),
    )
)

scored = (
    metrics_inventory
    .withColumn("running_next_last7_avg_quantity", F.sum("next_last7_avg_quantity").over(w10))
    .withColumn("prev_cumulative_last7_avg_quantity", F.lag("cumulative_last7_avg_quantity", 1).over(w11))
    .withColumn("running_last7_min_quantity_dense_rank", F.dense_rank().over(w12))
    .withColumn("event_seq", F.row_number().over(w13))
    .withColumn("event_seq_2", F.row_number().over(w14))
)

cleaned = (
    scored
    .filter(F.col("cumulative_last7_avg_quantity").between(668, 1075))
    .select(
        "id",
        "store_id",
        "last_restock",
        "last7_avg_quantity",
        "quantity_avg_3_prior",
        "opened_date",
        "next_last7_avg_quantity",
        "cumulative_last7_avg_quantity",
        "cumulative_quantity_avg_3_prior",
        "prev_cumulative_last7_avg_quantity",
        "running_last7_min_quantity_dense_rank",
        "event_seq_2",
        F.last_day("last_restock").alias("last_restock_month_end"),
    )
)

result = (
    cleaned
    .alias("src")
    .join(df_stores.alias("p"), F.col("src.store_id") == F.col("p.id"), "inner")
    .groupBy(F.col("p.city").alias("city"))
    .agg(
        F.count("*").alias("num_rows"),
        F.max(F.col("src.event_seq_2")).alias("max_event_seq_2"),
        F.sum(F.col("src.last7_avg_quantity")).alias("total_last7_avg_quantity"),
    )
)
