from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("quantity").desc(), "id")
w2 = Window.partitionBy("product_id", "store_id").orderBy("quantity", "id").rowsBetween(-5, Window.currentRow)
w3 = Window.orderBy("last_restock", "id")
w4 = Window.partitionBy("store_id", "product_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("product_id").orderBy("last_restock_dayofweek", "id")
w6 = Window.partitionBy("store_id").orderBy(F.col("row_seq").desc())
w7 = Window.partitionBy("store_id").orderBy(F.col("row_seq").desc(), "id")
w8 = Window.orderBy("cumulative_last_restock_dayofweek", "id").rowsBetween(-2, Window.currentRow)
w_running = Window.partitionBy("product_id", "store_id").orderBy(F.col("row_seq").asc_nulls_first())
w10 = Window.partitionBy("store_id").orderBy(F.col("distinct_product_ids").desc())
discount_by_store_id = df_orders.groupBy("store_id").agg(F.min("discount").alias("min_order_discount"))

cleaned = (
    df_inventory
    .filter(F.col("last_restock") > F.lit("2024-02-13").cast("date"))
    .select("*", F.dayofweek("last_restock").alias("last_restock_dayofweek"))
)

with_windows = (
    cleaned
    .select(
        "*",
        F.ntile(3).over(w).alias("quantity_bucket"),
        F.avg("quantity").over(w2).alias("quantity_avg_5"),
        F.row_number().over(w3).alias("row_seq"),
        F.sum("last_restock_dayofweek").over(w4).alias("cumulative_last_restock_dayofweek"),
    )
)

ranked = (
    with_windows
    .withColumn("rank_in_group", F.row_number().over(w5))
    .filter(F.col("rank_in_group") == 1)
)

latest = (
    ranked
    .withColumn("dedup_rank", F.rank().over(w6))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

latest_inventory = (
    latest
    .withColumn("rn", F.row_number().over(w7))
    .filter(F.col("rn") == 1)
)

metrics = (
    latest_inventory
    .withColumn("last_restock_dayofweek_avg_2", F.avg("last_restock_dayofweek").over(w8))
    .withColumn("row_seq_dense_rank", F.dense_rank().over(w_running))
)

summary = (
    metrics
    .groupBy("store_id", F.date_trunc("WEEK", "last_restock").alias("week_start"))
    .agg(
        F.bit_or("quantity").alias("quantity_bit_or"),
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("store_id").alias("distinct_store_ids"),
        F.countDistinct("product_id").alias("distinct_product_ids"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
    )
    .withColumn("distinct_product_ids_rank", F.rank().over(w10))
)

result = (
    summary
    .join(discount_by_store_id, on="store_id", how="left")
    .select(
        "store_id",
        "week_start",
        "quantity_bit_or",
        "distinct_ids",
        "distinct_store_ids",
        "distinct_product_ids",
        F.col("min_order_discount"),
    )
)
