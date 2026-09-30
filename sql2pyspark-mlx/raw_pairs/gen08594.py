from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id")
w2 = Window.partitionBy("order_id", "status")
w3 = Window.partitionBy(F.date_trunc("WEEK", "delivered_date"), "status")

valid = (
    df_shipments
    .filter((F.col("delivered_date") >= F.lit("2023-05-29").cast("date")) & F.col("order_id").isNotNull())
    .select(
        "id",
        "order_id",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        F.when(F.col("carrier") == "Kerry", 1).otherwise(0).alias("is_kerry"),
    )
)

remaining_keys = (
    valid
    .select("order_id")
    .subtract(
        df_shipments
        .filter(F.col("order_id").isin([10, 40]))
        .select("order_id")
    )
)

reconciled = (
    valid
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

base = (
    reconciled
    .filter(F.col("delivered_date") >= F.lit("2023-06-03").cast("date"))
    .select(
        "*",
        (F.datediff("shipped_date", "delivered_date") / 7).alias("weeks_delivered_date_to_shipped_date"),
    )
)

with_windows = (
    base
    .withColumn("order_id_avg_weeks_delivered_date_to_shipped_date", F.avg("weeks_delivered_date_to_shipped_date").over(w))
    .withColumn("order_id_max_is_kerry", F.max("is_kerry").over(w2))
    .withColumn("cost_share", F.round((F.col("cost") / F.sum("cost").over(w3)), 4))
    .withColumn("order_id_avg_is_kerry", F.avg("is_kerry").over(w))
)

cleaned = (
    with_windows
    .filter(F.col("order_id_avg_is_kerry").between(28, 59))
    .select(
        "id",
        "order_id",
        "delivered_date",
        "cost",
        "status",
        "is_kerry",
        "weeks_delivered_date_to_shipped_date",
        "order_id_max_is_kerry",
        "cost_share",
        (F.col("weeks_delivered_date_to_shipped_date") + F.col("cost_share")).alias("weeks_delivered_date_to_shipped_date_plus_cost_share"),
    )
)

result = (
    cleaned
    .alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .rollup(
        F.col("o.status").alias("order_status"),
        F.col("s.status").alias("shipment_status"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.sum(F.col("s.is_kerry")), 2).alias("total_is_kerry"),
    )
    .orderBy("order_status")
)
