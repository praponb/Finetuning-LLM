from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("order_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_payments
    .filter(F.col("amount").between(730, 830))
    .select(
        "id",
        "order_id",
        "method",
        "status",
        F.upper("status").alias("status_upper"),
        F.coalesce("method", F.lit("NONE")).alias("resolved_method"),
    )
)

base_payments = (
    base
    .filter(F.col("method") == "card")
    .select(
        "*",
        F.trim("method").alias("clean_method"),
        F.length("method").alias("method_length"),
    )
)

cleaned = (
    base_payments
    .filter(F.col("status").isin(["SUCCESS", "FAILED", "REFUNDED"]))
    .select(
        "*",
        F.round("method_length", 0).alias("method_length_rounded"),
        F.coalesce("method", F.lit("N/A")).alias("method_clean"),
    )
)

with_dims = (
    cleaned
    .alias("s")
    .join(df_orders.alias("p"), F.col("s.order_id") == F.col("p.id"), "left")
    .filter(F.col("p.order_date").between("2024-04-06", "2024-08-01"))
    .select(
        "s.id",
        "s.order_id",
        "s.method",
        "s.clean_method",
        "s.method_length",
        "s.method_length_rounded",
        F.col("p.order_date"),
    )
)

metrics = (
    with_dims
    .select(
        "id",
        "order_id",
        "clean_method",
        "method_length",
        "method_length_rounded",
        F.count("method_length").over(w).alias("running_count_method_length"),
        F.sum("method_length").over(w).alias("running_sum_method_length"),
        F.min("method_length").over(w).alias("running_min_method_length"),
    )
)

joined = (
    metrics
    .alias("s")
    .join(df_orders.alias("d"), F.col("s.order_id") == F.col("d.id"), "left")
    .select(
        "s.id",
        "s.order_id",
        "s.clean_method",
        "s.method_length",
        "s.method_length_rounded",
        "s.running_count_method_length",
        F.col("d.amount"),
        F.col("d.discount"),
    )
)

with_dims_payments = (
    joined
    .alias("a")
    .join(df_orders.alias("d"), F.col("a.order_id") == F.col("d.id"), "inner")
    .select("a.*", F.col("d.shipping_fee"), F.col("d.status"))
)

result = (
    with_dims_payments
    .withColumn("shipping_fee_tier", F.when(F.col("shipping_fee") >= 34, "LARGE").otherwise("SMALL"))
    .withColumn("method_length_tier", F.when(F.col("method_length") >= 68, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["shipping_fee_tier", "method_length_tier"], ["shipping_fee_tier"]],
        "shipping_fee_tier",
        "method_length_tier",
    )
    .agg(
        F.round(F.sum("shipping_fee") / F.nullif(F.count(F.when(F.col("status") == "PENDING", 1)), F.lit(0)), 2).alias("shipping_fee_per_pending"),
        F.count(F.when(F.col("status") == "PENDING", 1)).alias("pending_rows"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.countDistinct("id").alias("distinct_ids"),
        F.size(F.collect_set("status")).alias("num_status"),
        F.count("*").alias("num_rows"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy("distinct_ids")
)
