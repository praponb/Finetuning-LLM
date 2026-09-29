from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("store_id", "status")
w2 = Window.partitionBy("customer_id", "store_id").orderBy("amount", "id")
w3 = Window.partitionBy("status")
w_part_2 = Window.partitionBy("customer_id")
w5 = Window.partitionBy(F.date_trunc("MONTH", "order_date"), "status").orderBy(F.col("discount").asc_nulls_last())
w6 = Window.partitionBy("channel").orderBy(F.col("discount_dense_rank").asc_nulls_first(), "id")
w7 = Window.partitionBy("customer_id").orderBy(F.col("shipping_fee").desc(), "id")

valid = (
    df_orders
    .filter((F.col("order_date") >= F.lit("2023-06-09").cast("date")) & F.col("customer_id").isNotNull())
    .select("*", F.col("amount").cast("int").alias("amount_int"))
)

not_flagged = (
    valid
    .alias("s")
    .join(df_order_items.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.unit_price") < 350), "left_anti")
)

enriched = (
    not_flagged
    .select(
        "*",
        F.corr("discount", "shipping_fee").over(w_part).alias("corr_discount_shipping_fee"),
        F.lead("amount_int", 1).over(w2).alias("next_amount_int"),
        F.corr("shipping_fee", "amount").over(w3).alias("corr_shipping_fee_amount"),
        F.stddev_samp("amount_int").over(w_part_2).alias("customer_id_stddev_samp_amount_int"),
        F.dense_rank().over(w5).alias("discount_dense_rank"),
    )
)

deduped = (
    enriched
    .withColumn("dedup_rank", F.row_number().over(w6))
    .filter(F.col("dedup_rank") == 1)
)

deduped_orders = (
    deduped
    .withColumn("rn", F.row_number().over(w7))
    .filter(F.col("rn") == 1)
)

summary = (
    deduped_orders
    .groupBy("status")
    .agg(
        F.count(F.when(F.col("channel") == "web", 1)).alias("web_rows"),
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
        F.size(F.collect_set("channel")).alias("num_channels"),
        F.percentile_approx("discount", [0.25, 0.5, 0.75]).alias("discount_quartiles"),
        F.sum(F.when(F.col("channel") == "mobile", F.col("shipping_fee")).otherwise(0)).alias("mobile_shipping_fee"),
    )
)

result = (
    summary
    .select("status", "distinct_customer_ids", "num_channels", "mobile_shipping_fee")
    .orderBy(F.col("mobile_shipping_fee").desc())
)
