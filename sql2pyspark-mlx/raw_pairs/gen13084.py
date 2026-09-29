from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("product_id").orderBy(F.col("running_max_quantity").desc_nulls_first(), "id")
w3 = Window.partitionBy("product_id").orderBy(F.col("last_restock_plus_58d").desc(), "id")
w_hist = Window.partitionBy("product_id").orderBy("running_min_quantity", "id")

cleaned = (
    df_inventory
    .filter(F.col("last_restock") >= F.lit("2023-04-02").cast("date"))
    .select(
        "id",
        "product_id",
        "quantity",
        "last_restock",
        F.date_add("last_restock", 58).alias("last_restock_plus_58d"),
        F.date_trunc("YEAR", "last_restock").alias("last_restock_period"),
    )
)

scored = (
    cleaned
    .filter(F.month("last_restock_plus_58d").isin([3, 7, 8]))
    .select(
        "id",
        "product_id",
        "quantity",
        "last_restock_plus_58d",
        "last_restock_period",
        F.min("quantity").over(w_ordered).alias("running_min_quantity"),
        F.max("quantity").over(w_ordered).alias("running_max_quantity"),
        F.avg("quantity").over(w_ordered).alias("running_avg_quantity"),
        F.sum("quantity").over(w_ordered).alias("running_sum_quantity"),
    )
)

latest = (
    scored
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
)

latest_inventory = (
    latest
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

enriched = (
    latest_inventory
    .select(
        "id",
        "product_id",
        "last_restock_period",
        "running_min_quantity",
        "running_avg_quantity",
        "running_sum_quantity",
        "row_num",
        F.lag("running_avg_quantity").over(w_hist).alias("prev_running_avg_quantity"),
        F.lead("running_avg_quantity").over(w_hist).alias("next_running_avg_quantity"),
        F.lag("running_max_quantity").over(w_hist).alias("prev_running_max_quantity"),
        F.lead("running_max_quantity").over(w_hist).alias("next_running_max_quantity"),
        F.row_number().over(w_hist).alias("event_seq"),
    )
)

filtered = (
    enriched
    .filter(F.col("product_id").isin([2, 3, 5, 8]))
    .withColumn("last_restock_period_month", F.month("last_restock_period"))
)

result = (
    filtered
    .alias("a")
    .join(df_products.alias("p"), F.col("a.product_id") == F.col("p.id"), "inner")
    .groupBy(F.col("p.category").alias("category"), F.col("p.brand").alias("brand"))
    .agg(
        F.count("*").alias("rows_total"),
        F.round(F.sum(F.col("a.prev_running_avg_quantity")), 2).alias("total_prev_running_avg_quantity"),
    )
)
