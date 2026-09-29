from pyspark.sql import functions as F

remaining_keys = (
    df_orders
    .select("id", "customer_id")
    .subtract(
        df_orders
        .filter(F.col("customer_id").isin([17, 18, 22, 28]))
        .select("id", "customer_id")
    )
)

verified = (
    df_orders
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy("store_id")
    .agg(
        F.round(F.sum("shipping_fee") / F.nullif(F.count(F.when(F.col("status") == "COMPLETED", 1)), F.lit(0)), 2).alias("shipping_fee_per_completed"),
        F.countDistinct("channel").alias("distinct_channels"),
        F.round(F.sum("discount") / F.nullif(F.count(F.when(F.col("channel") == "mobile", 1)), F.lit(0)), 2).alias("discount_per_mobile"),
    )
    .filter(F.col("distinct_channels") > 1)
    .orderBy(F.col("discount_per_mobile").desc())
)
