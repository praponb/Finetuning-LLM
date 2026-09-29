from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("last_restock").desc_nulls_first(), "id")
w2 = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("product_id").orderBy("quantity", "id")
w4 = Window.orderBy("days_since_last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("quantity", "id").rowsBetween(-2, Window.currentRow)
w6 = Window.partitionBy("product_id").orderBy(F.col("prev_quantity").desc_nulls_first())
w7 = Window.partitionBy("product_id").orderBy(F.col("prev_quantity_dense_rank").desc_nulls_last())
revenue_by_product_id = df_store_sales.groupBy("product_id").agg(F.avg("revenue").alias("avg_store_sale_revenue"))

valid = (
    df_inventory
    .filter(F.col("store_id").isin([2, 3, 4]))
    .select(
        "id",
        "product_id",
        "quantity",
        "last_restock",
        F.datediff(F.current_date(), "last_restock").alias("days_since_last_restock"),
    )
)

ranked = (
    valid
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

enriched = (
    ranked
    .select(
        "*",
        F.sum("quantity").over(w2).alias("running_quantity"),
        F.lag("quantity", 1, 0).over(w_hist).alias("prev_quantity"),
        F.sum("quantity").over(w4).alias("cumulative_quantity"),
    )
)

with_windows = (
    enriched
    .select(
        "*",
        F.avg("quantity").over(w5).alias("quantity_avg_2"),
        F.dense_rank().over(w6).alias("prev_quantity_dense_rank"),
    )
)

cleaned = (
    with_windows
    .filter(F.col("last_restock") > F.lit("2024-02-19").cast("date"))
    .select(
        "id",
        "product_id",
        "quantity",
        "last_restock",
        "days_since_last_restock",
        "cumulative_quantity",
        "prev_quantity_dense_rank",
        F.floor("prev_quantity_dense_rank").alias("prev_quantity_dense_rank_whole"),
        F.when(F.col("quantity") >= 257, "High").when(F.col("quantity") >= 70, "Medium").otherwise("Low").alias("quantity_tier"),
    )
)

deduped = (
    cleaned
    .withColumn("latest_rank", F.rank().over(w7))
    .filter(F.col("latest_rank") == 1)
)

base = (
    deduped
    .filter(F.col("last_restock") > F.lit("2023-07-27").cast("date"))
    .select(
        "id",
        "product_id",
        "last_restock",
        "days_since_last_restock",
        "cumulative_quantity",
        "prev_quantity_dense_rank",
        "prev_quantity_dense_rank_whole",
        "latest_rank",
        (F.col("days_since_last_restock") - F.col("quantity")).alias("days_since_last_restock_minus_quantity"),
    )
)

summary = (
    base
    .groupBy("product_id")
    .agg(
        F.count("*").alias("cnt"),
        F.count("*").alias("event_count"),
        F.sum("prev_quantity_dense_rank_whole").alias("total_prev_quantity_dense_rank_whole"),
    )
    .filter(F.col("cnt") < 1)
)

result = (
    summary
    .join(revenue_by_product_id, on="product_id", how="left")
    .select("product_id", "cnt", F.col("avg_store_sale_revenue"))
)
