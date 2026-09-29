from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("week", "delivered_date"), "status").orderBy(F.col("cost").asc_nulls_first())
w2 = Window.partitionBy("order_id").orderBy("cost")

remaining_keys = (
    df_shipments
    .select("id", "carrier")
    .subtract(
        df_shipments
        .filter(F.col("delivered_date") > "2024-12-01")
        .select("id", "carrier")
    )
)

reconciled = (
    df_shipments
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.carrier") == F.col("k.carrier")), "inner")
    .select("s.*")
)

metrics = (
    reconciled
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "cost",
        "status",
        F.percent_rank().over(w).alias("cost_percent_rank"),
        F.cume_dist().over(w2).alias("cost_cume_dist"),
    )
)

joined = (
    metrics
    .alias("e")
    .join(df_orders.alias("o"), F.col("e.order_id") == F.col("o.id"), "left")
    .filter(F.col("o.amount") < 1250)
    .select("e.*", F.col("o.order_date"), F.col("o.amount"))
)

enriched = (
    joined
    .alias("e")
    .join(df_orders.alias("b"), F.col("e.order_id").eqNullSafe(F.col("b.id")), "left")
    .select(
        "e.id",
        "e.order_id",
        "e.shipped_date",
        "e.cost",
        "e.cost_percent_rank",
        "e.order_date",
        F.col("b.amount").alias("order_amount"),
    )
)

enriched_shipments = (
    enriched
    .alias("a")
    .join(df_orders.alias("b"), F.col("a.order_id") == F.col("b.id"), "left")
    .select(
        "a.order_id",
        "a.shipped_date",
        "a.order_date",
        F.col("b.order_date").alias("order_order_date"),
        F.col("b.status"),
    )
)

result = (
    enriched_shipments
    .groupBy(F.date_trunc("QUARTER", "order_date").alias("quarter_start"), "order_id")
    .agg(
        F.count_if(F.col("status") == "PENDING").alias("pending_events"),
        F.count(F.when(F.col("status") == "SHIPPED", 1)).alias("shipped_count"),
        F.size(F.collect_set("status")).alias("num_status"),
    )
    .filter(F.col("pending_events") > 1)
)
