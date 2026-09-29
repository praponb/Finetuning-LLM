from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("amount").desc_nulls_first())
w2 = Window.partitionBy("status", "method").orderBy("amount")

matched_keys = (
    df_payments
    .select("id")
    .exceptAll(
        df_payments
        .filter(F.col("status") == "REFUNDED")
        .select("id")
    )
)

kept_rows = (
    df_payments
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

with_dims = (
    kept_rows
    .alias("a")
    .join(df_orders.alias("d"), F.col("a.order_id") == F.col("d.id"), "left")
    .select("a.*", F.col("d.order_date"))
)

enriched = (
    with_dims
    .select(
        "*",
        F.rank().over(w).alias("amount_rank"),
        F.dense_rank().over(w2).alias("amount_dense_rank"),
    )
)

joined = (
    enriched
    .alias("s")
    .join(df_orders.alias("d"), F.col("s.order_id") == F.col("d.id"), "inner")
    .select("s.*", F.col("d.status").alias("order_status"))
)

result = (
    joined
    .groupingSets([["method", "order_id"], ["method"]], "method", "order_id")
    .agg(
        F.count(F.when(F.col("order_status") == "COMPLETED", 1)).alias("completed_rows"),
        F.countDistinct("status").alias("distinct_status"),
        F.sort_array(F.collect_set("order_status")).alias("order_status_list"),
        F.sum(F.when(F.col("status") == "FAILED", F.col("amount_rank")).otherwise(0)).alias("failed_amount_rank"),
        F.grouping("method").alias("grouping_id"),
    )
    .orderBy("method")
)
