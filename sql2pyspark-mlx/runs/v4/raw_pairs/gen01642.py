from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "channel").orderBy(F.col("amount").desc_nulls_last(), "id")
w2 = Window.orderBy("shipping_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy(F.col("shipping_fee").desc())
w_part = Window.partitionBy("region")
w5 = Window.partitionBy("channel").orderBy("discount", "id").rowsBetween(-6, -1)

ranked = (
    df_orders
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

with_dims = (
    ranked
    .alias("a")
    .join(df_stores.alias("d"), F.col("a.store_id") == F.col("d.id"), "left")
    .select("a.*", F.col("d.region"), F.col("d.store_name"))
)

joined = (
    with_dims
    .alias("e")
    .join(df_stores.alias("d"), F.col("e.store_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.city"))
)

filtered = (
    joined
    .filter(F.col("region").isNotNull())
    .select(
        "*",
        F.concat_ws("-", "status", "store_name").alias("status_store_name_key"),
    )
)

scored = (
    filtered
    .select(
        "*",
        F.sum("amount").over(w2).alias("cumulative_amount"),
        F.percent_rank().over(w3).alias("shipping_fee_percent_rank"),
        F.corr("shipping_fee", "discount").over(w_part).alias("corr_shipping_fee_discount"),
        F.avg("amount").over(w5).alias("amount_avg_6_prior"),
    )
)

result = (
    scored
    .select(
        "id",
        "customer_id",
        "shipping_fee_percent_rank",
        "amount",
        F.col("corr_shipping_fee_discount").alias("CORR_SHIPPING_FEE_DISCOUNT"),
    )
    .unpivot(
        ["id", "customer_id"],
        ["shipping_fee_percent_rank", "amount", "CORR_SHIPPING_FEE_DISCOUNT"],
        "measure",
        "value",
    )
    .filter(F.col("value").isNotNull())
)
