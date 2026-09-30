from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method", "status").orderBy("amount", "id")
w2 = Window.partitionBy("status").orderBy(F.col("amount_whole").desc(), "id")
cost_by_order_id = df_shipments.groupBy("order_id").agg(F.min("cost").alias("min_shipment_cost"))

valid = (
    df_payments
    .filter(F.col("order_id").isNotNull() & (F.col("method") != "card"))
    .select("*", F.ceil("amount").alias("amount_whole"))
)

deduped = (
    valid
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "order_id", "amount", "status", "amount_whole")
)

deduped_payments = (
    deduped
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

enriched = (
    deduped_payments
    .alias("src")
    .join(df_orders.alias("o"), F.col("src.order_id") == F.col("o.id"), "inner")
    .filter(F.col("o.shipping_fee").between(13, 21))
    .select("src.*", F.col("o.order_date"))
)

summary = (
    enriched
    .withColumn("amount_whole_tier", F.when(F.col("amount_whole") >= 83, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["amount_whole_tier", "order_id"], ["amount_whole_tier"]],
        "amount_whole_tier",
        "order_id",
    )
    .agg(
        F.round(F.sum("amount") / F.nullif(F.count(F.when(F.col("status") == "FAILED", 1)), F.lit(0)), 2).alias("amount_per_failed"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("event_count"),
        F.grouping("amount_whole_tier").alias("grouping_level"),
    )
)

result = (
    summary
    .join(cost_by_order_id, on="order_id", how="left")
    .select(
        "amount_whole_tier",
        "order_id",
        "amount_per_failed",
        "distinct_ids",
        "event_count",
        "grouping_level",
        F.col("min_shipment_cost"),
    )
    .orderBy(F.col("min_shipment_cost").desc_nulls_last())
)
