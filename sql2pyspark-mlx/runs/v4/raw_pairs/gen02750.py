from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("discount").desc(), "id")
w2 = Window.partitionBy("store_id").orderBy("shipping_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy(F.col("amount").desc())
discount_by_customer_id = df_orders.groupBy("customer_id").agg(F.max("discount").alias("max_order_discount"))

cleaned = (
    df_orders
    .filter(F.col("customer_id").isNotNull())
    .withColumn("status_title", F.initcap(F.trim("status")))
)

metrics = (
    cleaned
    .withColumn("discount_bucket", F.ntile(5).over(w))
    .withColumn("first_known_shipping_fee", F.first("shipping_fee", ignorenulls=True).over(w2))
    .withColumn("amount_dense_rank", F.dense_rank().over(w3))
)

not_flagged = (
    metrics
    .alias("s")
    .join(df_payments.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.paid_at") >= F.col("s.order_date")), "left_anti")
)

matched_keys = (
    not_flagged
    .select("customer_id", "id")
    .subtract(
        df_orders
        .filter(F.col("store_id").isNull())
        .select("customer_id", "id")
    )
)

verified = (
    not_flagged
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

kept_keys = (
    verified
    .select("store_id", "channel")
    .subtract(
        df_orders
        .filter(F.col("status").isin(["SHIPPED", "CANCELLED"]))
        .select("store_id", "channel")
    )
)

verified_orders = (
    verified
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")) & (F.col("s.channel") == F.col("k.channel")), "inner")
    .select("s.*")
)

pivoted = (
    verified_orders
    .groupBy("status", "customer_id")
    .pivot("channel", ["store", "web", "partner"])
    .agg(F.sum("discount"))
)

result = (
    pivoted
    .join(discount_by_customer_id, on="customer_id", how="left")
    .select("status", "customer_id", "store", "web", F.col("max_order_discount"))
    .orderBy(F.col("store").desc_nulls_last(), F.col("status").desc())
)
