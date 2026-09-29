from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("amount").desc(), "id")
w2 = Window.partitionBy("channel").orderBy(F.col("completed_events_2").desc())

valid = (
    df_orders
    .filter((F.col("order_date") >= F.lit("2023-09-02").cast("date")) & F.month("order_date").isin([1, 2, 9]))
    .select(
        "id",
        "store_id",
        "status",
        "channel",
        "amount",
        "discount",
        F.greatest("shipping_fee", "discount").alias("greatest_shipping_fee_discount"),
    )
)

deduped = (
    valid
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

joined = (
    deduped
    .alias("s")
    .join(df_stores.alias("p"), F.col("s.store_id") == F.col("p.id"), "left")
    .filter(F.col("p.city") != "Chiang Mai")
    .select("s.*", F.col("p.city"))
)

kept_keys = (
    joined
    .select("channel", "status")
    .intersectAll(
        df_orders
        .filter(F.col("status").isin(["PENDING", "RETURNED", "CANCELLED"]))
        .select("channel", "status")
    )
)

verified = (
    joined
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.channel") == F.col("k.channel")) & (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy(
        "channel",
        F.when(F.col("amount") >= 1430, "LARGE").otherwise("SMALL").alias("amount_tier"),
    )
    .agg(
        F.count_if(F.col("status") == "COMPLETED").alias("completed_events"),
        F.count_if(F.col("status") == "COMPLETED").alias("completed_events_2"),
        F.avg(F.when(F.col("city") == "Tokyo", F.col("amount"))).alias("tokyo_avg_amount"),
        F.count(F.when(F.col("city") == "New York", 1)).alias("new_york_count"),
        F.sum(F.when(F.col("channel") == "web", F.col("amount")).otherwise(0)).alias("web_amount"),
    )
    .withColumn("completed_events_2_percent_rank", F.percent_rank().over(w2))
)
