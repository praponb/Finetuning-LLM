from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("week_start").orderBy(F.col("corr_discount_amount").desc())

matched_keys = (
    df_orders
    .select("store_id", "channel")
    .intersect(
        df_orders
        .filter(F.col("shipping_fee") < 21)
        .select("store_id", "channel")
    )
)

verified = (
    df_orders
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")) & (F.col("s.channel") == F.col("k.channel")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy(
        F.date_trunc("week", "order_date").alias("week_start"),
        "customer_id",
        F.when(F.col("discount") >= 0.3, "PREMIUM").otherwise("STANDARD").alias("discount_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
        F.percentile_approx("amount", [0.25, 0.5, 0.75]).alias("amount_quartiles"),
        F.corr("discount", "amount").alias("corr_discount_amount"),
    )
    .withColumn("corr_discount_amount_rank", F.rank().over(w))
    .orderBy(F.col("customer_id").asc_nulls_last())
)
