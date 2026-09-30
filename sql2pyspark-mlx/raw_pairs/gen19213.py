from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("discount", "id")

filtered = (
    df_orders
    .filter(F.col("store_id").isNotNull() & (F.col("order_date") > F.to_date(F.lit("2023-07-19"))))
    .withColumn("channel_group", F.when(F.col("channel").isin(["partner", "store"]), "Group A").otherwise("Other"))
    .withColumn("channel_clean", F.coalesce("channel", F.lit("UNKNOWN")))
)

deduped = (
    filtered
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

matched_keys = (
    deduped
    .select("channel", "id")
    .exceptAll(
        df_orders
        .filter(F.col("customer_id") == 24)
        .select("channel", "id")
    )
)

verified = (
    deduped
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.channel") == F.col("k.channel")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

kept = (
    verified
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.delivered_date") < F.col("s.order_date")), "left_semi")
)

result = (
    kept
    .groupBy(
        F.when(F.col("amount") >= 1580, "HIGH").otherwise("LOW").alias("amount_tier"),
        F.date_trunc("WEEK", "order_date").alias("week_start"),
    )
    .agg(
        F.count(F.when(F.col("channel") == "web", 1)).alias("web_rows"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.count(F.when(F.col("status") == "COMPLETED", 1)).alias("completed_count"),
    )
)
