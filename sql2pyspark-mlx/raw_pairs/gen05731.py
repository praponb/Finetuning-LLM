from pyspark.sql import functions as F

matched_keys = (
    df_orders
    .select("id")
    .subtract(
        df_orders
        .filter(F.col("channel").isin(["partner", "store"]))
        .select("id")
    )
)

kept_rows = (
    df_orders
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .withColumn("week_start", F.date_trunc("WEEK", "order_date"))
    .groupingSets(
        [["customer_id", "week_start"], ["customer_id"], []],
        "customer_id",
        "week_start",
    )
    .agg(
        F.count(F.when(F.col("channel") == "mobile", 1)).alias("mobile_count"),
        F.sum(F.when(F.col("channel") == "mobile", F.col("shipping_fee")).otherwise(0)).alias("mobile_shipping_fee"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.percentile_approx("shipping_fee", 0.95).alias("shipping_fee_p95"),
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
        F.count(F.when(F.col("status") == "SHIPPED", 1)).alias("shipped_rows"),
    )
)
