from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("revenue", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("product_id").orderBy("last3_min_revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("product_id").orderBy("revenue", "id")
w4 = Window.partitionBy("product_id").orderBy("running_min_revenue", "id").rowsBetween(-5, Window.currentRow)
w5 = Window.partitionBy("product_id").orderBy(F.col("running_max_revenue_avg_5").desc_nulls_last(), "id")
w_hist = Window.partitionBy("store_id").orderBy("prev_running_sum_revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.orderBy("next_running_sum_revenue", "id")
w8 = Window.orderBy("next_running_sum_revenue_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("store_id")
w10 = Window.partitionBy("store_id").orderBy("prev_running_sum_revenue", "id").rowsBetween(-2, -1)
w11 = Window.orderBy("next_running_sum_revenue", "id").rowsBetween(-3, -1)
w12 = Window.orderBy("next_running_max_revenue", "id")
w13 = Window.partitionBy("store_id").orderBy(F.col("next_running_sum_revenue").desc(), "id")

enriched = (
    df_store_sales
    .withColumn("last3_sum_revenue", F.sum("revenue").over(w))
    .withColumn("last3_min_revenue", F.min("revenue").over(w))
    .withColumn("last3_avg_revenue", F.avg("revenue").over(w))
)

metrics = (
    enriched
    .select(
        "*",
        F.sum("revenue").over(w2).alias("running_sum_revenue"),
        F.max("revenue").over(w2).alias("running_max_revenue"),
        F.min("revenue").over(w2).alias("running_min_revenue"),
    )
)

enriched_store_sales = (
    metrics
    .filter(F.col("last3_sum_revenue") >= 19000)
    .select(
        "id",
        "store_id",
        "product_id",
        "units",
        "last3_avg_revenue",
        "running_max_revenue",
        "running_min_revenue",
        F.lag("running_max_revenue").over(w3).alias("prev_running_max_revenue"),
        F.lead("running_max_revenue").over(w3).alias("next_running_max_revenue"),
        F.lead("running_sum_revenue").over(w3).alias("next_running_sum_revenue"),
        F.lag("running_sum_revenue").over(w3).alias("prev_running_sum_revenue"),
        F.row_number().over(w3).alias("row_seq"),
        F.avg("running_max_revenue").over(w4).alias("running_max_revenue_avg_5"),
    )
)

deduped = (
    enriched_store_sales
    .withColumn("latest_rank", F.row_number().over(w5))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

metrics_store_sales = (
    deduped
    .select(
        "id",
        "store_id",
        "units",
        "last3_avg_revenue",
        "running_max_revenue",
        "prev_running_max_revenue",
        "next_running_max_revenue",
        "next_running_sum_revenue",
        "prev_running_sum_revenue",
        F.avg("prev_running_sum_revenue").over(w_hist).alias("running_avg_prev_running_sum_revenue"),
        F.max("prev_running_sum_revenue").over(w_hist).alias("running_max_prev_running_sum_revenue"),
        F.sum("prev_running_sum_revenue").over(w_hist).alias("running_sum_prev_running_sum_revenue"),
        F.ntile(5).over(w_part).alias("next_running_sum_revenue_bucket"),
    )
)

enriched_store_sales_2 = (
    metrics_store_sales
    .withColumn("cumulative_running_sum_prev_running_sum_revenue", F.sum("running_sum_prev_running_sum_revenue").over(w8))
    .withColumn("last3_avg_revenue_share", F.round((F.col("last3_avg_revenue") / F.sum("last3_avg_revenue").over(w9)), 4))
    .withColumn("next_running_max_revenue_avg_2_prior", F.avg("next_running_max_revenue").over(w10))
    .withColumn("prev_running_sum_revenue_avg_3_prior", F.avg("prev_running_sum_revenue").over(w11))
    .withColumn("event_seq", F.row_number().over(w12))
)

deduped_store_sales = (
    enriched_store_sales_2
    .withColumn("latest_rank", F.row_number().over(w13))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

result = (
    deduped_store_sales
    .select(
        "id",
        "units",
        "running_max_revenue",
        "prev_running_max_revenue",
        "next_running_max_revenue",
        "next_running_sum_revenue",
        "prev_running_sum_revenue",
        "last3_avg_revenue_share",
        "prev_running_sum_revenue_avg_3_prior",
    )
    .orderBy(F.col("prev_running_max_revenue").desc())
)
