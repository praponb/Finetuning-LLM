from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("least_unit_price_quantity").desc_nulls_last(), "id")
w_hist = Window.partitionBy("product_id").orderBy("unit_price_rounded", "id").rowsBetween(-2, Window.currentRow)
w3 = Window.partitionBy("product_id")
w4 = Window.partitionBy("product_id").orderBy("last3_avg_unit_price_rounded", "id")
w5 = Window.partitionBy("product_id").orderBy(F.col("unit_price_rounded").asc_nulls_first(), "id")
w6 = Window.orderBy("last3_avg_unit_price_rounded", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy("product_id_avg_quantity", "id")

valid = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        F.least("unit_price", "quantity").alias("least_unit_price_quantity"),
        F.round("unit_price", 0).alias("unit_price_rounded"),
    )
)

latest = (
    valid
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "product_id", "quantity", "unit_price", "unit_price_rounded")
)

metrics = (
    latest
    .filter(F.col("unit_price_rounded").isNotNull())
    .select(
        "*",
        F.min("unit_price_rounded").over(w_hist).alias("last3_min_unit_price_rounded"),
        F.sum("unit_price_rounded").over(w_hist).alias("last3_sum_unit_price_rounded"),
        F.avg("unit_price_rounded").over(w_hist).alias("last3_avg_unit_price_rounded"),
        F.avg("quantity").over(w3).alias("product_id_avg_quantity"),
    )
)

deduped = (
    metrics
    .withColumn("latest_rank", F.row_number().over(w4))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

deduped_order_items = (
    deduped
    .withColumn("row_num", F.row_number().over(w5))
    .filter(F.col("row_num") == 1)
    .select(
        "id",
        "unit_price_rounded",
        "last3_avg_unit_price_rounded",
        "product_id_avg_quantity",
    )
)

with_windows = (
    deduped_order_items
    .select(
        "*",
        F.sum("unit_price_rounded").over(w6).alias("running_unit_price_rounded"),
        F.ntile(10).over(w7).alias("product_id_avg_quantity_bucket"),
    )
)

valid_order_items = (
    with_windows
    .filter(F.col("last3_avg_unit_price_rounded") < 63)
    .select(
        "id",
        "last3_avg_unit_price_rounded",
        "product_id_avg_quantity",
        "running_unit_price_rounded",
        F.when(F.col("product_id_avg_quantity") >= 8, "High").when(F.col("product_id_avg_quantity") >= 3, "Medium").otherwise("Low").alias("product_id_avg_quantity_level"),
    )
)

result = (
    valid_order_items
    .withColumn("running_unit_price_rounded_tier", F.when(F.col("running_unit_price_rounded") >= 402, "PREMIUM").otherwise("STANDARD"))
    .rollup("running_unit_price_rounded_tier")
    .agg(
        F.count("*").alias("row_count"),
        F.count("*").alias("cnt"),
        F.sum("product_id_avg_quantity").alias("total_product_id_avg_quantity"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("running_unit_price_rounded", [0.25, 0.5, 0.75]).alias("running_unit_price_rounded_quartiles"),
        F.grouping_id().alias("grouping_id"),
    )
)
