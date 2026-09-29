from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.orderBy("shipping_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.orderBy("discount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status")
w4 = Window.partitionBy("status").orderBy(F.col("order_date_dayofweek").asc_nulls_last())
w5 = Window.partitionBy("channel").orderBy("first_known_shipping_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("channel").orderBy(F.col("order_date_dayofweek").desc(), "id")
w7 = Window.partitionBy("store_id").orderBy("order_date_plus_24d", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("status", "customer_id").orderBy("order_date_dayofweek_percent_rank", "id")
w9 = Window.partitionBy("store_id").orderBy("cumulative_discount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_orders
    .filter((~F.col("channel").eqNullSafe(F.lit("partner"))) & (F.col("amount") <= 730))
    .select(
        "*",
        F.dayofweek("order_date").alias("order_date_dayofweek"),
        F.date_add("order_date", 24).alias("order_date_plus_24d"),
    )
)

metrics = (
    valid
    .filter(F.col("amount") < 1260)
    .select(
        "*",
        F.sum("discount").over(w_grp).alias("cumulative_discount"),
        F.first("shipping_fee", ignorenulls=True).over(w_part).alias("first_known_shipping_fee"),
    )
)

metrics_orders = (
    metrics
    .withColumn("corr_shipping_fee_discount", F.corr("shipping_fee", "discount").over(w3))
    .withColumn("order_date_dayofweek_percent_rank", F.percent_rank().over(w4))
    .withColumn("running_first_known_shipping_fee", F.sum("first_known_shipping_fee").over(w5))
    .withColumn("order_date_dayofweek_bucket", F.ntile(3).over(w6))
    .withColumn("last_known_order_date_dayofweek", F.last("order_date_dayofweek", ignorenulls=True).over(w7))
)

enriched = (
    metrics_orders
    .select(
        "id",
        "customer_id",
        "amount",
        "discount",
        "shipping_fee",
        "corr_shipping_fee_discount",
        "order_date_dayofweek_percent_rank",
        F.lead("shipping_fee", 1).over(w8).alias("next_shipping_fee"),
        F.sum("last_known_order_date_dayofweek").over(w9).alias("cumulative_last_known_order_date_dayofweek"),
    )
)

result = (
    enriched
    .alias("a")
    .join(df_customers.alias("dim"), F.col("a.customer_id") == F.col("dim.id"), "inner")
    .cube(F.col("dim.segment").alias("segment"))
    .agg(
        F.count("*").alias("rows_total"),
        F.round(F.sum(F.col("a.corr_shipping_fee_discount")), 2).alias("total_corr_shipping_fee_discount"),
        F.min(F.col("a.next_shipping_fee")).alias("min_next_shipping_fee"),
    )
    .orderBy(F.col("segment").asc_nulls_first())
)
