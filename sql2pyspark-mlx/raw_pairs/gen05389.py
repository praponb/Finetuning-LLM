from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "method").orderBy("amount_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("method").orderBy("amount_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.to_date("paid_at"))
w_part = Window.partitionBy(F.year("paid_at"), "method").orderBy("paid_at_to_date_count_amount_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy(F.to_date("paid_at"), "method").orderBy(F.col("paid_at_to_date_count_amount_whole").desc(), "id")
w6 = Window.partitionBy("order_id", "status").orderBy("amount", "id")
w7 = Window.partitionBy("method")

valid = (
    df_payments
    .filter((F.col("status") == "SUCCESS") & F.col("method").isNotNull())
    .withColumn("method_status_key", F.concat_ws(",", "method", "status"))
    .withColumn("amount_whole", F.ceil("amount"))
    .withColumn("method_filled", F.coalesce("method", F.lit("UNKNOWN")))
)

filtered = (
    valid
    .filter(F.col("method").isNotNull() & (F.col("method") != "cash"))
    .withColumn("amount_whole_whole", F.floor("amount_whole"))
)

enriched = (
    filtered
    .select(
        "*",
        F.first("amount_whole_whole", ignorenulls=True).over(w).alias("first_known_amount_whole_whole"),
        F.first("amount", ignorenulls=True).over(w2).alias("first_known_amount"),
        F.count("amount_whole").over(w3).alias("paid_at_to_date_count_amount_whole"),
    )
)

with_windows = (
    enriched
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "status",
        "amount_whole",
        "method_filled",
        "amount_whole_whole",
        "first_known_amount_whole_whole",
        "first_known_amount",
        "paid_at_to_date_count_amount_whole",
        F.sum("first_known_amount").over(w_part).alias("cumulative_first_known_amount"),
        F.ntile(10).over(w5).alias("paid_at_to_date_count_amount_whole_bucket"),
        F.ntile(10).over(w6).alias("amount_bucket"),
        F.round((F.col("first_known_amount") / F.sum("first_known_amount").over(w7)), 4).alias("first_known_amount_share"),
    )
)

result = (
    with_windows
    .withColumn("first_known_amount_whole_whole_tier", F.when(F.col("first_known_amount_whole_whole") >= 86, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["first_known_amount_whole_whole_tier", "order_id"], ["first_known_amount_whole_whole_tier"]],
        "first_known_amount_whole_whole_tier",
        "order_id",
    )
    .agg(
        F.max("first_known_amount").alias("max_first_known_amount"),
        F.percentile_approx("first_known_amount_share", 0.95).alias("first_known_amount_share_p95"),
        F.sum(F.when(F.col("method") == "card", F.col("amount"))).alias("card_total_amount"),
        F.sum(F.when(F.col("method") == "card", F.col("amount_whole")).otherwise(0)).alias("card_amount_whole"),
        F.grouping("first_known_amount_whole_whole_tier").alias("grouping_id"),
    )
    .filter(F.col("first_known_amount_share_p95") > 0)
)
