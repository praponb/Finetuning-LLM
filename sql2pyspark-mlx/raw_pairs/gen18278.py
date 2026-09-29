from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id", "category").orderBy(F.col("launch_date").desc(), "id")
w2 = Window.partitionBy("brand").orderBy("stock_qty", "id")
w_recent = Window.partitionBy("brand").orderBy("prev_cost", "id")
w4 = Window.partitionBy("brand").orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("brand").orderBy("row_seq_2", "id")
w6 = Window.partitionBy("brand").orderBy("prev_stock_qty_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

deduped = (
    df_products
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "brand", "cost", "stock_qty")
)

with_windows = (
    deduped
    .withColumn("next_cost", F.lead("cost").over(w2))
    .withColumn("prev_cost", F.lag("cost").over(w2))
    .withColumn("next_stock_qty", F.lead("stock_qty").over(w2))
    .withColumn("prev_stock_qty", F.lag("stock_qty").over(w2))
    .withColumn("row_seq", F.row_number().over(w2))
)

with_windows_products = (
    with_windows
    .filter(F.col("next_cost").between(210, 710))
    .select(
        "*",
        F.lag("stock_qty").over(w_recent).alias("prev_stock_qty_2"),
        F.lead("stock_qty").over(w_recent).alias("next_stock_qty_2"),
        F.lag("prev_cost").over(w_recent).alias("prev_prev_cost"),
        F.lead("prev_cost").over(w_recent).alias("next_prev_cost"),
        F.row_number().over(w_recent).alias("row_seq_2"),
        F.first("prev_cost", ignorenulls=True).over(w4).alias("first_known_prev_cost"),
    )
)

ranked = (
    with_windows_products
    .withColumn("dedup_rank", F.row_number().over(w5))
    .filter(F.col("dedup_rank") == 1)
    .select(
        "id",
        "brand",
        "prev_cost",
        "next_stock_qty",
        "prev_stock_qty",
        "prev_stock_qty_2",
        "next_stock_qty_2",
        "next_prev_cost",
        "row_seq_2",
    )
)

scored = (
    ranked
    .withColumn("running_sum_prev_cost", F.sum("prev_cost").over(w6))
    .withColumn("running_avg_prev_cost", F.avg("prev_cost").over(w6))
    .withColumn("running_min_prev_cost", F.min("prev_cost").over(w6))
    .withColumn("running_count_prev_cost", F.count("prev_cost").over(w6))
)

result = (
    scored
    .groupBy("brand")
    .agg(
        F.count(F.when(F.col("brand") == "Umbrella", 1)).alias("umbrella_rows"),
        F.count("*").alias("cnt"),
        F.percentile_approx("prev_stock_qty_2", 0.9).alias("prev_stock_qty_2_p90"),
        F.round(F.sum("next_stock_qty_2") / F.nullif(F.count(F.when(F.col("brand") == "Initech", 1)), F.lit(0)), 2).alias("next_stock_qty_2_per_initech"),
        F.count(F.when(F.col("brand") == "Globex", 1)).alias("globex_count"),
        F.sort_array(F.collect_set("brand")).alias("brand_list"),
    )
    .filter(F.col("next_stock_qty_2_per_initech") > 0)
    .orderBy(F.col("prev_stock_qty_2_p90").asc_nulls_first())
)
