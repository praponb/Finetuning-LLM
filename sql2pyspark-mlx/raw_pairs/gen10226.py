from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("product_id")
w2 = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("first_known_unit_price", "id").rowsBetween(-5, Window.currentRow)
w5 = Window.orderBy(F.col("running_sum_unit_price").desc(), "id")
w6 = Window.orderBy("running_count_unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy("first_known_running_count_unit_price", "id")
w_recent = Window.orderBy(F.col("next_running_sum_unit_price_bucket").asc_nulls_first())
w9 = Window.orderBy("next_first_known_running_count_unit_price", "id").rowsBetween(-3, -1)

with_windows = (
    df_order_items
    .select(
        "id",
        "unit_price",
        F.corr("quantity", "unit_price").over(w_hist).alias("corr_quantity_unit_price"),
        F.first("unit_price", ignorenulls=True).over(w2).alias("first_known_unit_price"),
    )
)

scored = (
    with_windows
    .filter(F.col("first_known_unit_price").between(180, 200))
    .select(
        "id",
        "unit_price",
        F.count("unit_price").over(w3).alias("running_count_unit_price"),
        F.sum("unit_price").over(w3).alias("running_sum_unit_price"),
        F.avg("unit_price").over(w3).alias("running_avg_unit_price"),
        F.avg("unit_price").over(w4).alias("unit_price_avg_5"),
    )
)

with_windows_order_items = (
    scored
    .select(
        "id",
        "unit_price",
        F.ntile(5).over(w5).alias("running_sum_unit_price_bucket"),
        F.first("running_count_unit_price", ignorenulls=True).over(w6).alias("first_known_running_count_unit_price"),
    )
)

scored_order_items = (
    with_windows_order_items
    .select(
        "id",
        "unit_price",
        F.lead("first_known_running_count_unit_price").over(w7).alias("next_first_known_running_count_unit_price"),
        F.lead("running_sum_unit_price_bucket").over(w7).alias("next_running_sum_unit_price_bucket"),
        F.row_number().over(w7).alias("seq_no"),
    )
)

base = (
    scored_order_items
    .filter(F.col("unit_price") > 140)
    .withColumn("next_first_known_running_count_unit_price_whole", F.ceil("next_first_known_running_count_unit_price"))
)

metrics = (
    base
    .select(
        "*",
        F.rank().over(w_recent).alias("next_running_sum_unit_price_bucket_rank"),
        F.avg("next_running_sum_unit_price_bucket").over(w9).alias("next_running_sum_unit_price_bucket_avg_3_prior"),
    )
)

result = (
    metrics
    .groupBy("seq_no")
    .agg(
        F.percentile_approx("next_running_sum_unit_price_bucket", 0.9).alias("next_running_sum_unit_price_bucket_p90"),
        F.min("next_first_known_running_count_unit_price").alias("min_next_first_known_running_count_unit_price"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .filter(F.col("min_next_first_known_running_count_unit_price") < 1000000)
)
