from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("channel").orderBy("order_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy(F.date_trunc("MONTH", "order_date"), "customer_id").orderBy("shipping_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
discount_by_store_id = df_orders.groupBy("store_id").agg(F.sum("discount").alias("sum_order_discount"))

kept = (
    df_orders
    .alias("s")
    .join(df_order_items.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.product_id").isNull()), "left_anti")
)

metrics = (
    kept
    .filter(F.col("discount") < 0.13)
    .select(
        "id",
        "store_id",
        "amount",
        "discount",
        "shipping_fee",
        F.count("discount").over(w_running).alias("running_count_discount"),
        F.max("discount").over(w_running).alias("running_max_discount"),
        F.sum("discount").over(w_running).alias("running_sum_discount"),
        F.min("discount").over(w_running).alias("running_min_discount"),
        F.sum("shipping_fee").over(w_grp).alias("cumulative_shipping_fee"),
    )
)

matched_keys = (
    metrics
    .select("id")
    .subtract(
        df_orders
        .filter(F.col("discount") >= 0.1)
        .select("id")
    )
)

verified = (
    metrics
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

valid = (
    verified
    .filter(F.col("store_id").isin([21, 23]))
    .select(
        "id",
        "store_id",
        "amount",
        "discount",
        "shipping_fee",
        "running_sum_discount",
        "cumulative_shipping_fee",
        F.when(F.col("running_sum_discount") >= 1.99, "High").when(F.col("running_sum_discount") >= 0.88, "Medium").otherwise("Low").alias("running_sum_discount_tier"),
        F.when(F.col("shipping_fee") >= 31, "High").when(F.col("shipping_fee") >= 18, "Medium").otherwise("Low").alias("shipping_fee_level"),
    )
)

summary = (
    valid
    .groupBy("store_id")
    .agg(
        F.percentile_approx("cumulative_shipping_fee", [0.25, 0.5, 0.75]).alias("cumulative_shipping_fee_quartiles"),
        F.max("shipping_fee").alias("max_shipping_fee"),
        F.avg("shipping_fee").alias("avg_shipping_fee"),
        F.sum("cumulative_shipping_fee").alias("total_cumulative_shipping_fee"),
        F.max("amount").alias("max_amount"),
        F.percentile_approx("discount", 0.9).alias("discount_p90"),
    )
)

result = (
    summary
    .join(discount_by_store_id, on="store_id", how="left")
    .select(
        "store_id",
        "avg_shipping_fee",
        "total_cumulative_shipping_fee",
        "max_amount",
        "discount_p90",
        F.col("sum_order_discount"),
    )
    .orderBy(F.col("avg_shipping_fee").desc_nulls_first())
)
