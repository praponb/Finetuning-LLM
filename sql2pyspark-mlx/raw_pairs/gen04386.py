from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "product_id").orderBy(F.col("quantity").asc_nulls_first(), "id")
w2 = Window.partitionBy("order_id").orderBy(F.col("quantity").desc(), "id")
w3 = Window.orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy(F.col("quantity").asc_nulls_first())
w5 = Window.orderBy("quantity_dense_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("running_quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy("last_known_quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy("running_quantity_dense_rank_2", "id").rowsBetween(-5, -1)
w_ordered = Window.orderBy("running_quantity_dense_rank", "id").rowsBetween(-3, -1)
w10 = Window.orderBy(F.col("running_quantity_dense_rank_3").desc_nulls_last())

latest = (
    df_order_items
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "order_id", "quantity")
)

deduped = (
    latest
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
    .select("id", "quantity")
)

with_windows = (
    deduped
    .withColumn("running_quantity", F.sum("quantity").over(w3))
    .withColumn("last_known_quantity", F.last("quantity", ignorenulls=True).over(w3))
    .withColumn("quantity_dense_rank", F.dense_rank().over(w4))
)

metrics = (
    with_windows
    .filter(F.col("quantity_dense_rank") != 3)
    .select(
        "id",
        "quantity",
        "running_quantity",
        "last_known_quantity",
        F.sum("quantity_dense_rank").over(w5).alias("running_quantity_dense_rank"),
        F.sum("quantity_dense_rank").over(w6).alias("running_quantity_dense_rank_2"),
        F.sum("quantity_dense_rank").over(w7).alias("running_quantity_dense_rank_3"),
    )
)

scored = (
    metrics
    .select(
        "id",
        "running_quantity",
        "last_known_quantity",
        "running_quantity_dense_rank",
        F.avg("running_quantity_dense_rank_3").over(w8).alias("running_quantity_dense_rank_3_avg_5_prior"),
        F.avg("last_known_quantity").over(w_ordered).alias("last_known_quantity_avg_3_prior"),
        F.last("running_quantity_dense_rank", ignorenulls=True).over(w7).alias("last_known_running_quantity_dense_rank"),
        F.percent_rank().over(w10).alias("running_quantity_dense_rank_3_percent_rank"),
    )
)

result = (
    scored
    .groupBy(
        F.when(F.col("running_quantity") >= 33, "PREMIUM").otherwise("STANDARD").alias("running_quantity_tier"),
    )
    .agg(
        F.round(F.avg("running_quantity"), 2).alias("avg_running_quantity"),
        F.percentile_approx("last_known_quantity_avg_3_prior", [0.25, 0.5, 0.75]).alias("last_known_quantity_avg_3_prior_quartiles"),
    )
    .filter(F.col("avg_running_quantity") >= 0)
    .orderBy(F.col("avg_running_quantity").desc_nulls_first())
)
