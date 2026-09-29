from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "channel").orderBy("discount", "id")
w2 = Window.partitionBy("customer_id")
w3 = Window.partitionBy("store_id")
w4 = Window.partitionBy("store_id").orderBy("seq_no", "id")
w5 = Window.partitionBy("customer_id").orderBy(F.col("seq_no").desc())
w6 = Window.partitionBy("customer_id").orderBy("order_date_period", "id")
w7 = Window.partitionBy("customer_id").orderBy("order_date_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_orders
    .filter((F.col("order_date") > F.lit("2023-03-13").cast("date")) & F.col("amount").between(440, 560))
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "amount",
        "discount",
        F.date_trunc("week", "order_date").alias("order_date_period"),
        F.concat_ws("-", "channel", "status").alias("channel_status_key"),
        F.coalesce("channel", F.lit("UNKNOWN")).alias("channel_clean"),
    )
)

enriched = (
    base
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "amount",
        "discount",
        "order_date_period",
        "channel_clean",
        F.row_number().over(w).alias("seq_no"),
        F.round((F.col("amount") / F.sum("amount").over(w2)), 4).alias("amount_share"),
        F.corr("discount", "amount").over(w3).alias("corr_discount_amount"),
    )
)

ranked = (
    enriched
    .withColumn("latest_rank", F.row_number().over(w4))
    .filter(F.col("latest_rank") == 1)
    .select(
        "id",
        "customer_id",
        "order_date",
        "status",
        "order_date_period",
        "channel_clean",
        "seq_no",
        "amount_share",
        "corr_discount_amount",
    )
)

deduped = (
    ranked
    .withColumn("row_num", F.rank().over(w5))
    .filter(F.col("row_num") == 1)
)

flagged = (
    deduped
    .withColumn("session_start_flag", F.when(F.col("order_date_period") - F.lag("order_date_period").over(w6) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

islands = flagged.withColumn("island_id", F.sum("session_start_flag").over(w7))

stats = (
    islands
    .rollup("island_id", F.date_trunc("YEAR", "order_date_period").alias("year_start"))
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.count("*").alias("event_count"),
        F.count("*").alias("cnt"),
    )
)

result = (
    stats
    .select("island_id", "year_start", "status_list", "cnt")
    .orderBy(F.col("year_start").asc_nulls_last(), "cnt")
)
