from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("sale_date").desc(), "id")
w2 = Window.partitionBy("product_id")
w3 = Window.partitionBy("store_id").orderBy(F.col("revenue").desc())
w4 = Window.partitionBy("store_id", "product_id").orderBy("revenue_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("store_id", "product_id")
w6 = Window.partitionBy("product_id", "store_id").orderBy(F.col("revenue").asc_nulls_last())
w7 = Window.partitionBy("store_id", "product_id").orderBy("revenue_rank", "id")

cleaned = (
    df_store_sales
    .filter(F.col("sale_date") <= "2024-05-01")
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "revenue",
        F.when(F.col("revenue") >= 4400, "High").when(F.col("revenue") >= 1500, "Medium").otherwise("Low").alias("revenue_band"),
    )
)

metrics = (
    cleaned
    .filter(F.col("store_id") == 4)
    .withColumn("row_seq", F.row_number().over(w))
    .withColumn("product_id_sum_revenue", F.sum("revenue").over(w2))
    .withColumn("revenue_rank", F.rank().over(w3))
)

metrics_store_sales = (
    metrics
    .select(
        "*",
        F.last("row_seq", ignorenulls=True).over(w4).alias("last_known_row_seq"),
        F.round((F.col("revenue_rank") / F.sum("revenue_rank").over(w5)), 4).alias("revenue_rank_share"),
        F.dense_rank().over(w6).alias("revenue_dense_rank"),
    )
)

deduped = (
    metrics_store_sales
    .withColumn("dedup_rank", F.row_number().over(w7))
    .filter(F.col("dedup_rank") == 1)
)

totals = (
    deduped
    .rollup(
        F.when(F.col("revenue_rank") >= 8, "HIGH").otherwise("LOW").alias("revenue_rank_tier"),
        "product_id",
    )
    .agg(
        F.percentile_approx("row_seq", 0.95).alias("row_seq_p95"),
        F.max("revenue_dense_rank").alias("max_revenue_dense_rank"),
        F.min("revenue").alias("min_revenue"),
    )
    .filter(F.col("row_seq_p95") >= 0)
)

result = totals.select("revenue_rank_tier", "max_revenue_dense_rank", "min_revenue")
