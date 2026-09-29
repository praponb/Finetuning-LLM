from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "product_id")
w2 = Window.partitionBy("order_id").orderBy("least_unit_price_quantity", "id").rowsBetween(-3, -1)
w3 = Window.partitionBy("product_id").orderBy(F.col("unit_price").desc_nulls_last(), "id")
w_running = Window.partitionBy("order_id").orderBy("rank_in_group", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("product_id", "order_id").orderBy(F.col("unit_price_avg_3_prior").desc(), "id")
w6 = Window.partitionBy("product_id").orderBy(F.col("unit_price_avg_3_prior").desc(), "id")

valid = (
    df_order_items
    .filter(F.col("unit_price") > 300)
    .select("*", F.least("unit_price", "quantity").alias("least_unit_price_quantity"))
)

with_windows = (
    valid
    .withColumn("order_id_stddev_samp_unit_price", F.stddev_samp("unit_price").over(w))
    .withColumn("unit_price_avg_3_prior", F.avg("unit_price").over(w2))
)

ranked = (
    with_windows
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
)

enriched = (
    ranked
    .withColumn("running_max_unit_price_avg_3_prior", F.max("unit_price_avg_3_prior").over(w_running))
    .withColumn("running_count_unit_price_avg_3_prior", F.count("unit_price_avg_3_prior").over(w_running))
    .withColumn("running_sum_unit_price_avg_3_prior", F.sum("unit_price_avg_3_prior").over(w_running))
    .withColumn("row_seq", F.row_number().over(w_part))
)

valid_order_items = (
    enriched
    .filter(F.col("product_id").isNotNull())
    .withColumn("running_count_unit_price_avg_3_prior_plus_unit_price", (F.col("running_count_unit_price_avg_3_prior") + F.col("unit_price")))
)

cleaned = (
    valid_order_items
    .filter(F.col("quantity") > 7)
    .select(
        "id",
        "product_id",
        "quantity",
        "unit_price",
        "least_unit_price_quantity",
        "unit_price_avg_3_prior",
        "rank_in_group",
        "running_max_unit_price_avg_3_prior",
        "running_count_unit_price_avg_3_prior",
        "running_sum_unit_price_avg_3_prior",
        "running_count_unit_price_avg_3_prior_plus_unit_price",
        F.round("quantity", 2).alias("quantity_rounded"),
    )
)

ranked_order_items = (
    cleaned
    .withColumn("latest_rank", F.row_number().over(w6))
    .filter(F.col("latest_rank") == 1)
)

result = (
    ranked_order_items
    .groupBy(
        F.when(F.col("running_sum_unit_price_avg_3_prior") >= 1560, "LARGE").otherwise("SMALL").alias("running_sum_unit_price_avg_3_prior_tier"),
    )
    .agg(
        F.percentile_approx("quantity_rounded", [0.25, 0.5, 0.75]).alias("quantity_rounded_quartiles"),
        F.avg("quantity").alias("avg_quantity"),
        F.count("*").alias("event_count"),
        F.bit_and("quantity").alias("quantity_bit_and"),
    )
)
