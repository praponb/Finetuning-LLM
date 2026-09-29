from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("order_status", "status").orderBy("running_sum_cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status", "order_id").orderBy(F.col("amount").desc())
w4 = Window.partitionBy("carrier").orderBy(F.col("running_avg_cost").desc_nulls_first())
w_part = Window.partitionBy("carrier").orderBy("running_max_shipping_fee", "id")
w6 = Window.partitionBy("carrier")
w_ordered = Window.partitionBy("carrier").orderBy("running_max_shipping_fee", "id").rowsBetween(-6, -1)

scored = (
    df_shipments
    .filter(F.col("status") == "LOST")
    .select(
        "*",
        F.count("cost").over(w).alias("running_count_cost"),
        F.avg("cost").over(w).alias("running_avg_cost"),
        F.sum("cost").over(w).alias("running_sum_cost"),
    )
)

joined = (
    scored
    .alias("e")
    .join(df_orders.alias("b"), F.col("e.order_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.shipping_fee"))
)

with_dims = (
    joined
    .alias("e")
    .join(df_orders.alias("p"), F.col("e.order_id") == F.col("p.id"), "left")
    .select("e.*", F.col("p.amount"), F.col("p.status").alias("order_status"))
)

scored_shipments = (
    with_dims
    .select(
        "id",
        "carrier",
        "running_avg_cost",
        "shipping_fee",
        "amount",
        F.max("shipping_fee").over(w2).alias("running_max_shipping_fee"),
        F.min("shipping_fee").over(w2).alias("running_min_shipping_fee"),
        F.count("shipping_fee").over(w2).alias("running_count_shipping_fee"),
        F.cume_dist().over(w3).alias("amount_cume_dist"),
    )
)

latest = (
    scored_shipments
    .withColumn("rn", F.rank().over(w4))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    latest
    .select(
        "id",
        "running_avg_cost",
        "shipping_fee",
        F.ntile(5).over(w_part).alias("running_max_shipping_fee_bucket"),
        F.round((F.col("running_avg_cost") / F.sum("running_avg_cost").over(w6)), 4).alias("running_avg_cost_share"),
        F.avg("running_count_shipping_fee").over(w_ordered).alias("running_count_shipping_fee_avg_6_prior"),
    )
)
