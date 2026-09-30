from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("order_id")
w3 = Window.orderBy(F.col("unit_price").desc(), "id")
w4 = Window.partitionBy("product_id", "order_id").orderBy("unit_price", "id")
w5 = Window.partitionBy("order_id").orderBy(F.col("unit_price_bucket").desc_nulls_first(), "id")
w6 = Window.partitionBy("order_id").orderBy("unit_price_bucket", "id")
w_running = Window.partitionBy(F.month("order_date"), "order_id").orderBy("order_id_stddev_samp_unit_price", "id")
w_grp = Window.partitionBy("order_id").orderBy(F.col("cumulative_unit_price").desc())

scored = (
    df_order_items
    .filter(F.col("order_id") == 40)
    .select(
        "id",
        "order_id",
        "quantity",
        "unit_price",
        F.sum("unit_price").over(w).alias("cumulative_unit_price"),
        F.stddev_samp("unit_price").over(w2).alias("order_id_stddev_samp_unit_price"),
        F.row_number().over(w3).alias("event_seq"),
        F.ntile(4).over(w4).alias("unit_price_bucket"),
        F.corr("unit_price", "quantity").over(w2).alias("corr_unit_price_quantity"),
    )
)

joined = (
    scored
    .alias("a")
    .join(df_orders.alias("d"), F.col("a.order_id") == F.col("d.id"), "left")
    .select("a.*", F.col("d.order_date"))
)

deduped = (
    joined
    .withColumn("latest_rank", F.row_number().over(w5))
    .filter(F.col("latest_rank") == 1)
)

ranked = (
    deduped
    .withColumn("dedup_rank", F.row_number().over(w6))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

scored_order_items = (
    ranked
    .withColumn("order_id_stddev_samp_unit_price_bucket", F.ntile(3).over(w_running))
    .withColumn("cumulative_unit_price_dense_rank", F.dense_rank().over(w_grp))
)

stats = (
    scored_order_items
    .cube(
        F.date_trunc("YEAR", "order_date").alias("year_start"),
        F.date_trunc("MONTH", "order_date").alias("activity_month"),
    )
    .agg(
        F.countDistinct("order_id").alias("distinct_order_ids"),
        F.bit_and("quantity").alias("quantity_bit_and"),
        F.percentile_approx("order_id_stddev_samp_unit_price_bucket", 0.9).alias("order_id_stddev_samp_unit_price_bucket_p90"),
    )
    .filter(F.col("order_id_stddev_samp_unit_price_bucket_p90") < 1000000)
)

result = (
    stats
    .select(
        "year_start",
        "activity_month",
        "distinct_order_ids",
        "quantity_bit_and",
        "order_id_stddev_samp_unit_price_bucket_p90",
    )
    .orderBy(F.col("order_id_stddev_samp_unit_price_bucket_p90").desc_nulls_last())
)
