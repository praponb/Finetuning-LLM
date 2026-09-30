from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("cost")
w2 = Window.partitionBy("carrier", "order_id").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.month("delivered_date"), "carrier").orderBy("cost", "id").rowsBetween(-3, -1)

metrics = (
    df_shipments
    .select(
        "id",
        "order_id",
        "carrier",
        "cost",
        "status",
        F.dense_rank().over(w).alias("cost_dense_rank"),
        F.first("carrier", ignorenulls=True).over(w2).alias("first_known_carrier"),
        F.avg("cost").over(w3).alias("cost_avg_3_prior"),
    )
)

joined = (
    metrics
    .alias("s")
    .join(df_orders.alias("d"), F.col("s.order_id") == F.col("d.id"), "left")
    .filter(F.col("d.amount") >= 1070)
    .select("s.*", F.col("d.status").alias("order_status"))
)

with_dims = (
    joined
    .alias("src")
    .join(df_orders.alias("o"), F.col("src.order_id") == F.col("o.id"), "left")
    .select(
        "src.*",
        F.col("o.status").alias("order_status_2"),
        F.col("o.discount"),
        F.col("o.shipping_fee"),
    )
)

remaining_keys = (
    with_dims
    .select("status")
    .subtract(
        df_shipments
        .filter(F.month("delivered_date").isin([5, 6, 7]))
        .select("status")
    )
)

verified = (
    with_dims
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

result = (
    verified
    .alias("a")
    .join(df_orders.alias("p"), F.col("a.order_id") == F.col("p.id"), "inner")
    .groupingSets(
        [["order_status", "order_status_2"], ["order_status"]],
        F.col("p.status").alias("order_status"),
        F.col("a.order_status_2").alias("order_status_2"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.avg(F.col("a.cost_avg_3_prior")), 2).alias("avg_cost_avg_3_prior"),
        F.grouping_id().alias("gid"),
    )
    .orderBy("order_status")
)
