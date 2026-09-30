from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy("stock_qty", "id")
w2 = Window.orderBy("cost", "id").rowsBetween(-6, Window.currentRow)
w3 = Window.orderBy(F.col("cost").desc())
w4 = Window.orderBy("last7_avg_cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("last7_max_cost")
w6 = Window.orderBy(F.col("cost").desc_nulls_first())

deduped = (
    df_products
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "cost", "stock_qty", "tags")
)

not_flagged = (
    deduped
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.store_id").isin([3, 4, 5, 6])), "left_anti")
)

with_windows = (
    not_flagged
    .select(
        "id",
        "cost",
        "stock_qty",
        "tags",
        F.avg("cost").over(w2).alias("last7_avg_cost"),
        F.sum("cost").over(w2).alias("last7_sum_cost"),
        F.max("cost").over(w2).alias("last7_max_cost"),
        F.dense_rank().over(w3).alias("cost_dense_rank"),
    )
)

metrics = (
    with_windows
    .select(
        "id",
        "stock_qty",
        "tags",
        "last7_avg_cost",
        "last7_sum_cost",
        "cost_dense_rank",
        F.sum("cost").over(w4).alias("cumulative_cost"),
        F.cume_dist().over(w5).alias("last7_max_cost_cume_dist"),
        F.cume_dist().over(w6).alias("cost_cume_dist"),
    )
)

filtered = (
    metrics
    .filter(F.col("last7_max_cost_cume_dist") <= 0.3)
    .select(
        "*",
        F.when(F.col("stock_qty") >= 322, "High").when(F.col("stock_qty") >= 155, "Medium").otherwise("Low").alias("stock_qty_band"),
        F.when(F.col("last7_max_cost_cume_dist") >= 0.78, "High").when(F.col("last7_max_cost_cume_dist") >= 0.22, "Medium").otherwise("Low").alias("last7_max_cost_cume_dist_level"),
    )
)

result = (
    filtered
    .select(
        "id",
        "stock_qty",
        "tags",
        "last7_avg_cost",
        "cost_dense_rank",
        "cumulative_cost",
        "cost_cume_dist",
    )
)
