from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("amount", "id").rowsBetween(-2, -1)
w2 = Window.orderBy(F.col("order_amount").desc_nulls_first())
w3 = Window.partitionBy("order_id")
w4 = Window.partitionBy("order_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("order_id").orderBy("order_amount")
w6 = Window.orderBy(F.col("amount").desc_nulls_last())
w7 = Window.orderBy("order_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_shipments
    .filter(F.col("delivered_date") >= "2023-10-01")
    .select(
        "id",
        "order_id",
        "cost",
        "status",
        F.length("carrier").alias("carrier_length"),
    )
)

enriched = (
    cleaned
    .alias("a")
    .join(df_orders.alias("p"), F.col("a.order_id").eqNullSafe(F.col("p.id")), "left")
    .filter(F.col("p.status") != "SHIPPED")
    .select(
        "a.id",
        "a.order_id",
        "a.cost",
        "a.status",
        "a.carrier_length",
        F.col("p.amount"),
    )
)

with_dims = (
    enriched
    .alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .select("s.id", "s.order_id", "s.amount", F.col("o.amount").alias("order_amount"))
)

metrics = (
    with_dims
    .select(
        "id",
        "amount",
        "order_amount",
        F.avg("order_amount").over(w).alias("order_amount_avg_2_prior"),
        F.cume_dist().over(w2).alias("order_amount_cume_dist"),
        F.round((F.col("amount") / F.sum("amount").over(w3)), 4).alias("amount_share"),
        F.first("order_amount", ignorenulls=True).over(w4).alias("first_known_order_amount"),
        F.rank().over(w5).alias("order_amount_rank"),
    )
)

valid = (
    metrics
    .filter(F.col("order_amount_avg_2_prior") < 896)
    .withColumn("order_amount_avg_2_prior_times_order_amount_cume_dist", (F.col("order_amount_avg_2_prior") * F.col("order_amount_cume_dist")))
)

valid_shipments = (
    valid
    .filter(F.col("amount") >= 1510)
    .select(
        "id",
        "amount",
        "order_amount",
        "order_amount_cume_dist",
        "amount_share",
        "order_amount_rank",
        "order_amount_avg_2_prior_times_order_amount_cume_dist",
        (F.col("order_amount_cume_dist") + F.col("first_known_order_amount")).alias("order_amount_cume_dist_plus_first_known_order_amount"),
        (F.col("order_amount_avg_2_prior") * F.col("first_known_order_amount")).alias("order_amount_avg_2_prior_times_first_known_order_amount"),
    )
)

with_windows = (
    valid_shipments
    .select(
        "id",
        "amount",
        "order_amount_cume_dist",
        "amount_share",
        "order_amount_rank",
        "order_amount_avg_2_prior_times_first_known_order_amount",
        F.rank().over(w6).alias("amount_rank"),
        F.last("order_amount_avg_2_prior_times_order_amount_cume_dist", ignorenulls=True).over(w7).alias("last_known_order_amount_avg_2_prior_times_order_amount_cume_dist"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "amount",
        "order_amount_rank",
        "order_amount_avg_2_prior_times_first_known_order_amount",
        "amount_rank",
    )
)
