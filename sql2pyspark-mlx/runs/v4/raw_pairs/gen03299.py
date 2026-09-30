from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "channel").orderBy(F.col("amount").desc_nulls_first(), "id")
w2 = Window.partitionBy("customer_id", "channel").orderBy(F.col("amount").desc_nulls_last(), "id")
w3 = Window.partitionBy("channel").orderBy("discount", "id").rowsBetween(-3, -1)
w_hist = Window.partitionBy("store_id", "status").orderBy("discount", "id")
w5 = Window.partitionBy("status").orderBy("dedup_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("channel").orderBy(F.col("num_channels").desc())

joined = (
    df_orders
    .alias("src")
    .join(df_stores.alias("s"), F.col("src.store_id") == F.col("s.id"), "inner")
    .select("src.*", F.col("s.store_name"))
)

latest = (
    joined
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

latest_orders = (
    latest
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
)

metrics = (
    latest_orders
    .withColumn("shipping_fee_avg_3_prior", F.avg("shipping_fee").over(w3))
    .withColumn("discount_bucket", F.ntile(10).over(w_hist))
    .withColumn("running_discount", F.sum("discount").over(w5))
)

result = (
    metrics
    .groupBy(
        "channel",
        F.when(F.col("amount") >= 1680, "PREMIUM").otherwise("STANDARD").alias("amount_tier"),
    )
    .agg(
        F.round(F.sum("running_discount") / F.nullif(F.count(F.when(F.col("channel") == "store", 1)), F.lit(0)), 2).alias("running_discount_per_store"),
        F.round(F.sum("shipping_fee_avg_3_prior") / F.nullif(F.count(F.when(F.col("channel") == "partner", 1)), F.lit(0)), 2).alias("shipping_fee_avg_3_prior_per_partner"),
        F.size(F.collect_set("channel")).alias("num_channels"),
    )
    .withColumn("num_channels_dense_rank", F.dense_rank().over(w6))
)
