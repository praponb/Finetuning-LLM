from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("order_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("channel").orderBy("amount", "id")

metrics = (
    df_orders
    .filter(F.col("shipping_fee") < 37)
    .select(
        "*",
        F.last("shipping_fee", ignorenulls=True).over(w).alias("last_known_shipping_fee"),
        F.ntile(5).over(w_hist).alias("amount_bucket"),
    )
)

result = (
    metrics
    .withColumn("amount_tier", F.when(F.col("amount") >= 1690, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["amount_tier", "channel"], ["amount_tier"], []],
        "amount_tier",
        "channel",
    )
    .agg(
        F.sum(F.when(F.col("status") == "COMPLETED", F.col("amount")).otherwise(0)).alias("completed_amount"),
        F.percentile_approx("discount", [0.25, 0.5, 0.75]).alias("discount_quartiles"),
        F.max(F.when(F.col("status") == "SHIPPED", F.col("amount_bucket"))).alias("shipped_max_amount_bucket"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.round(F.sum("discount") / F.nullif(F.count(F.when(F.col("status") == "COMPLETED", 1)), F.lit(0)), 2).alias("discount_per_completed"),
        F.grouping("amount_tier").alias("grouping_id"),
    )
    .filter(F.col("avg_amount") > 0)
    .orderBy(F.col("channel").desc())
)
