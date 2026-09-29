from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "customer_id")
w2 = Window.partitionBy("channel").orderBy(F.col("discount").desc(), "id")
w_ordered = Window.partitionBy("customer_id", "channel")
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.unit_price").desc(), F.col("c.id").desc())
w5 = Window.partitionBy("store_id").orderBy("shipping_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("channel").orderBy("discount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_orders
    .withColumn("status_count_shipping_fee", F.count("shipping_fee").over(w))
    .withColumn("discount_bucket", F.ntile(10).over(w2))
    .withColumn("corr_amount_shipping_fee", F.corr("amount", "shipping_fee").over(w_ordered))
)

latest_match = (
    enriched
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.quantity"))
)

enriched_orders = (
    latest_match
    .withColumn("cumulative_corr_amount_shipping_fee", F.sum("corr_amount_shipping_fee").over(w5))
    .withColumn("last_known_status_count_shipping_fee", F.last("status_count_shipping_fee", ignorenulls=True).over(w6))
)

summary = (
    enriched_orders
    .withColumn("week_start", F.date_trunc("WEEK", "order_date"))
    .withColumn("year_start", F.date_trunc("YEAR", "order_date"))
    .cube("week_start", "year_start")
    .agg(
        F.countDistinct("store_id").alias("distinct_store_ids"),
        F.round(F.sum("corr_amount_shipping_fee") / F.nullif(F.count(F.when(F.col("status") == "RETURNED", 1)), F.lit(0)), 2).alias("corr_amount_shipping_fee_per_returned"),
        F.size(F.collect_set("status")).alias("num_status"),
        F.grouping_id().alias("grouping_level"),
    )
    .filter(F.col("num_status") < 1)
)

result = summary.select("week_start", "year_start", "num_status")
