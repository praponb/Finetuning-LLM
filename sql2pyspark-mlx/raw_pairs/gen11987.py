from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.paid_at").desc(), F.col("c.id").desc())

with_last = (
    df_orders
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.store_id",
        "s.channel",
        "s.amount",
        "s.shipping_fee",
        F.col("c.paid_at").alias("payment_paid_at"),
    )
)

result = (
    with_last
    .groupBy(
        F.when(F.col("shipping_fee") >= 40, "PREMIUM").otherwise("STANDARD").alias("shipping_fee_tier"),
    )
    .agg(
        F.count(F.when(F.col("channel") == "partner", 1)).alias("partner_rows"),
        F.percentile_approx("amount", 0.5).alias("amount_p50"),
        F.count(F.when(F.col("channel") == "partner", 1)).alias("partner_count"),
    )
    .orderBy(F.col("shipping_fee_tier").asc_nulls_last())
)
