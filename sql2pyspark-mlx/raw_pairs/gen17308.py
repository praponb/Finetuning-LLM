from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("store_id", "product_id").orderBy("last_restock", "id")
w2 = Window.partitionBy("store_id").orderBy(F.col("last_restock").desc_nulls_first(), "id")
w_running = Window.partitionBy("store_id").orderBy("prev_quantity", "id").rowsBetween(-3, Window.currentRow)

cleaned = (
    df_inventory
    .filter(F.col("store_id").isin([4, 5, 6]))
    .select(
        "*",
        F.date_format("last_restock", "yyyy-MM-dd").alias("last_restock_label"),
    )
)

scored = (
    cleaned
    .filter(F.col("product_id") == 2)
    .select(
        "*",
        F.lead("quantity").over(w_recent).alias("next_quantity"),
        F.lag("quantity").over(w_recent).alias("prev_quantity"),
        F.row_number().over(w_recent).alias("row_seq"),
    )
)

latest = (
    scored
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
)

scored_inventory = (
    latest
    .select(
        "id",
        "product_id",
        "last_restock_label",
        "next_quantity",
        "prev_quantity",
        "rn",
        F.max("quantity").over(w_running).alias("last4_max_quantity"),
        F.sum("quantity").over(w_running).alias("last4_sum_quantity"),
        F.count("quantity").over(w_running).alias("last4_count_quantity"),
    )
)

joined = (
    scored_inventory
    .alias("src")
    .join(df_products.alias("p"), F.col("src.product_id") == F.col("p.id"), "left")
    .select(
        "src.id",
        "src.product_id",
        "src.next_quantity",
        "src.last4_max_quantity",
        "src.last4_sum_quantity",
        "src.last4_count_quantity",
        F.col("p.product_name"),
        F.col("p.launch_date"),
    )
)

result = (
    joined
    .groupBy(
        F.when(F.col("next_quantity") >= 182, "LARGE").otherwise("SMALL").alias("next_quantity_tier"),
        F.when(F.col("last4_count_quantity") >= 13, "HIGH").otherwise("LOW").alias("last4_count_quantity_tier"),
        "product_id",
    )
    .agg(
        F.min("last4_count_quantity").alias("min_last4_count_quantity"),
        F.min("next_quantity").alias("min_next_quantity"),
        F.percentile_approx("last4_sum_quantity", 0.5).alias("last4_sum_quantity_p50"),
        F.round(F.avg("last4_count_quantity"), 2).alias("avg_last4_count_quantity"),
        F.min("last4_sum_quantity").alias("min_last4_sum_quantity"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy(F.col("min_last4_sum_quantity").desc_nulls_last())
)
