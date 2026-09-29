from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "order_id").orderBy("amount")
w_ordered = Window.partitionBy("method").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w3 = Window.partitionBy(F.month("paid_at"), "channel").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w4 = Window.partitionBy(F.year("paid_at"), "method").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-7776000, Window.currentRow)

latest = (
    df_payments
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
)

kept_keys = (
    latest
    .select("status", "method")
    .intersect(
        df_payments
        .filter(F.col("paid_at") < "2025-10-01")
        .select("status", "method")
    )
)

reconciled = (
    latest
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.method") == F.col("k.method")), "inner")
    .select("s.*")
)

enriched = (
    reconciled
    .alias("src")
    .join(df_orders.alias("o"), F.col("src.order_id").eqNullSafe(F.col("o.id")), "left")
    .filter(F.col("o.channel") == "mobile")
    .select("src.*", F.col("o.channel"), F.col("o.discount"), F.col("o.shipping_fee"))
)

enriched_payments = (
    enriched
    .select(
        "id",
        "method",
        "amount",
        "status",
        "row_num",
        "channel",
        "discount",
        "shipping_fee",
        F.sum("discount").over(w_ordered).alias("discount_sum_30d"),
        F.count("shipping_fee").over(w3).alias("shipping_fee_count_7d"),
        F.count("discount").over(w4).alias("discount_count_90d"),
    )
)

result = (
    enriched_payments
    .select(
        "id",
        "amount",
        "status",
        "row_num",
        "shipping_fee",
        "discount_sum_30d",
        "discount_count_90d",
    )
    .orderBy(F.col("discount_count_90d").desc())
)
