from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("order_date").desc(), "id")
w2 = Window.partitionBy("customer_id").orderBy("is_shipped", "id")

ranked = (
    df_orders
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "amount",
        "shipping_fee",
    )
)

kept = (
    ranked
    .alias("s")
    .join(df_order_items.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.quantity") < 5), "left_anti")
)

base = (
    kept
    .filter(F.col("order_date") >= F.lit("2023-05-20").cast("date"))
    .select(
        "id",
        "customer_id",
        "status",
        "amount",
        "shipping_fee",
        F.when(F.col("status") == "SHIPPED", 1).otherwise(0).alias("is_shipped"),
    )
)

latest = (
    base
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
)

result = (
    latest
    .groupBy(
        F.when(F.col("is_shipped") >= 67, "HIGH").otherwise("LOW").alias("is_shipped_tier"),
    )
    .agg(
        F.sum(F.when(F.col("status") == "PENDING", F.col("amount")).otherwise(0)).alias("pending_amount"),
        F.count(F.when(F.col("status") == "PENDING", 1)).alias("pending_count"),
        F.percentile_approx("is_shipped", 0.95).alias("is_shipped_p95"),
        F.round(F.sum("shipping_fee") / F.nullif(F.count(F.when(F.col("status") == "PENDING", 1)), F.lit(0)), 2).alias("shipping_fee_per_pending"),
        F.percentile_approx("is_shipped", 0.95).alias("is_shipped_p95_2"),
    )
    .orderBy(F.col("pending_amount").desc_nulls_last())
)
