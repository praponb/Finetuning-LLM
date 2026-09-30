from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("carrier").orderBy("delivered_date", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("status").orderBy(F.col("shipped_date").desc(), "id")
w3 = Window.partitionBy("order_id").orderBy(F.col("discount").desc_nulls_last())

filtered = (
    df_shipments
    .filter(F.col("carrier").isNotNull() & (F.col("carrier").eqNullSafe(F.lit("FedEx"))))
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        F.coalesce("carrier", F.lit("N/A")).alias("carrier_filled"),
        F.date_trunc("YEAR", "delivered_date").alias("delivered_date_period"),
    )
)

metrics = (
    filtered
    .select(
        "id",
        "order_id",
        "shipped_date",
        "status",
        F.max("cost").over(w_running).alias("last3_max_cost"),
        F.min("cost").over(w_running).alias("last3_min_cost"),
        F.count("cost").over(w_running).alias("last3_count_cost"),
        F.row_number().over(w2).alias("event_seq"),
    )
)

enriched = (
    metrics
    .alias("s")
    .join(df_orders.alias("p"), F.col("s.order_id") == F.col("p.id"), "inner")
    .select(
        "s.id",
        "s.order_id",
        "s.last3_max_cost",
        "s.last3_min_cost",
        "s.last3_count_cost",
        "s.event_seq",
        F.col("p.discount"),
    )
)

ranked = (
    enriched
    .withColumn("rank_in_group", F.rank().over(w3))
    .filter(F.col("rank_in_group") == 1)
    .select(
        "id",
        "order_id",
        "last3_max_cost",
        "last3_count_cost",
        "event_seq",
        "discount",
    )
)

result = ranked.select("id", "last3_max_cost", "last3_count_cost", "discount")
