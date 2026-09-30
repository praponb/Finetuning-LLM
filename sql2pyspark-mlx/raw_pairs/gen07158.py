from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("quantity").desc(), "id")
w2 = Window.orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.year("last_restock"))
w_running = Window.orderBy("last_restock", "id")
w5 = Window.orderBy("last_restock_year_sum_quantity", "id")
w6 = Window.partitionBy(F.year("last_restock")).orderBy("event_seq", "id")

valid = (
    df_inventory
    .filter((F.col("last_restock") >= F.lit("2023-05-28").cast("date")) & F.col("product_id").isin([2, 4, 6, 8]))
    .select(
        "id",
        "quantity",
        "last_restock",
        F.when(F.col("quantity") >= 183, "High").when(F.col("quantity") >= 42, "Medium").otherwise("Low").alias("quantity_band"),
    )
)

enriched = (
    valid
    .select(
        "id",
        "quantity",
        "last_restock",
        F.ntile(4).over(w).alias("quantity_bucket"),
        F.sum("quantity").over(w2).alias("cumulative_quantity"),
        F.sum("quantity").over(w3).alias("last_restock_year_sum_quantity"),
    )
)

scored = (
    enriched
    .select(
        "*",
        F.lead("quantity_bucket").over(w_running).alias("next_quantity_bucket"),
        F.lag("quantity_bucket").over(w_running).alias("prev_quantity_bucket"),
        F.lead("last_restock_year_sum_quantity").over(w_running).alias("next_last_restock_year_sum_quantity"),
        F.lag("last_restock_year_sum_quantity").over(w_running).alias("prev_last_restock_year_sum_quantity"),
        F.row_number().over(w_running).alias("event_seq"),
        F.row_number().over(w5).alias("row_seq"),
    )
)

scored_inventory = (
    scored
    .select(
        "id",
        "quantity",
        "quantity_bucket",
        "last_restock_year_sum_quantity",
        "prev_quantity_bucket",
        "next_last_restock_year_sum_quantity",
        "prev_last_restock_year_sum_quantity",
        "event_seq",
        "row_seq",
        F.min("last_restock_year_sum_quantity").over(w2).alias("running_min_last_restock_year_sum_quantity"),
        F.max("last_restock_year_sum_quantity").over(w2).alias("running_max_last_restock_year_sum_quantity"),
        F.avg("last_restock_year_sum_quantity").over(w2).alias("running_avg_last_restock_year_sum_quantity"),
        F.count("last_restock_year_sum_quantity").over(w2).alias("running_count_last_restock_year_sum_quantity"),
        F.ntile(3).over(w6).alias("event_seq_bucket"),
    )
)

base = (
    scored_inventory
    .filter(F.col("quantity") <= 204)
    .select(
        "id",
        "quantity",
        "quantity_bucket",
        "last_restock_year_sum_quantity",
        "prev_quantity_bucket",
        "next_last_restock_year_sum_quantity",
        "prev_last_restock_year_sum_quantity",
        "event_seq",
        "row_seq",
        "running_min_last_restock_year_sum_quantity",
        "running_max_last_restock_year_sum_quantity",
        "running_count_last_restock_year_sum_quantity",
        "event_seq_bucket",
        (F.col("row_seq") / F.nullif("last_restock_year_sum_quantity", F.lit(0))).alias("row_seq_per_last_restock_year_sum_quantity"),
    )
)

valid_inventory = (
    base
    .filter(F.col("quantity") != 207)
    .withColumn("running_count_last_restock_year_sum_quantity_times_row_seq_per_last_restock_year_sum_quantity", (F.col("running_count_last_restock_year_sum_quantity") * F.col("row_seq_per_last_restock_year_sum_quantity")))
    .withColumn("next_last_restock_year_sum_quantity_rounded", F.round("next_last_restock_year_sum_quantity", 1))
)

result = (
    valid_inventory
    .select(
        "id",
        "quantity",
        "quantity_bucket",
        "prev_quantity_bucket",
        "event_seq",
        "running_min_last_restock_year_sum_quantity",
        "running_max_last_restock_year_sum_quantity",
        "running_count_last_restock_year_sum_quantity",
        "event_seq_bucket",
    )
    .orderBy(F.col("id").desc_nulls_last())
)
