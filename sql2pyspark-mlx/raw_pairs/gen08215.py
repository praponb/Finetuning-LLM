from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "channel").orderBy("order_date", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.orderBy("shipping_fee", "id")
w3 = Window.partitionBy("channel", "status").orderBy("last3_sum_shipping_fee", "id")
w4 = Window.partitionBy("store_id").orderBy("shipping_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("order_date", "id").rowsBetween(-3, -1)
w6 = Window.orderBy(F.col("row_seq").asc_nulls_first())
w7 = Window.partitionBy("city").orderBy(F.col("last_known_prev_amount").desc_nulls_last())

metrics = (
    df_orders
    .filter(F.col("customer_id").isin([1, 17, 21]))
    .withColumn("last3_avg_shipping_fee", F.avg("shipping_fee").over(w))
    .withColumn("last3_sum_shipping_fee", F.sum("shipping_fee").over(w))
    .withColumn("last3_min_shipping_fee", F.min("shipping_fee").over(w))
    .withColumn("prev_amount", F.lag("amount", 1).over(w2))
)

enriched = (
    metrics
    .select(
        "id",
        "store_id",
        "order_date",
        "shipping_fee",
        "last3_avg_shipping_fee",
        "last3_sum_shipping_fee",
        "last3_min_shipping_fee",
        F.row_number().over(w3).alias("row_seq"),
        F.last("prev_amount", ignorenulls=True).over(w4).alias("last_known_prev_amount"),
    )
)

with_dims = (
    enriched
    .alias("s")
    .join(df_stores.alias("p"), F.col("s.store_id") == F.col("p.id"), "inner")
    .select("s.*", F.col("p.sqft"), F.col("p.city"), F.col("p.region"))
)

with_windows = (
    with_dims
    .select(
        "id",
        "store_id",
        "order_date",
        "last3_avg_shipping_fee",
        "last3_sum_shipping_fee",
        "row_seq",
        "sqft",
        "city",
        "region",
        F.avg("last3_min_shipping_fee").over(w5).alias("last3_min_shipping_fee_avg_3_prior"),
        F.percent_rank().over(w6).alias("row_seq_percent_rank"),
        F.dense_rank().over(w7).alias("last_known_prev_amount_dense_rank"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "last3_avg_shipping_fee",
        "last3_sum_shipping_fee",
        "row_seq",
        "sqft",
        "city",
        "region",
        "row_seq_percent_rank",
    )
    .orderBy("city")
)
