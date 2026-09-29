from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "carrier").orderBy(F.col("cost").asc_nulls_first())
w2 = Window.partitionBy("carrier", "order_id").orderBy(F.col("amount").asc_nulls_first(), "id")
w3 = Window.partitionBy("status")
w4 = Window.partitionBy("status").orderBy("cost_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy(F.month("shipped_date"), "status").orderBy("order_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

ranked = (
    df_shipments
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
)

with_dims = (
    ranked
    .alias("a")
    .join(df_orders.alias("d"), F.col("a.order_id") == F.col("d.id"), "left")
    .select(
        "a.id",
        "a.order_id",
        "a.carrier",
        "a.shipped_date",
        "a.delivered_date",
        "a.cost",
        "a.status",
        "a.rn",
        F.col("d.amount"),
    )
)

ranked_shipments = (
    with_dims
    .withColumn("rn_2", F.row_number().over(w2))
    .filter(F.col("rn_2") == 1)
)

enriched = (
    ranked_shipments
    .alias("s")
    .join(df_orders.alias("p"), F.col("s.order_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.order_date"))
)

base = (
    enriched
    .filter(F.col("order_id").isNotNull() & (F.col("delivered_date") >= F.to_date(F.lit("2023-04-28"))))
    .select(
        "id",
        "shipped_date",
        "cost",
        "status",
        "amount",
        "rn_2",
        "order_date",
        F.ceil("cost").alias("cost_whole"),
    )
)

scored = (
    base
    .withColumn("cost_share", F.round((F.col("cost") / F.sum("cost").over(w3)), 4))
    .withColumn("amount_share", F.round((F.col("amount") / F.sum("amount").over(w3)), 4))
    .withColumn("last_known_amount", F.last("amount", ignorenulls=True).over(w4))
    .withColumn("cost_share_2", F.round((F.col("cost") / F.sum("cost").over(w3)), 4))
    .withColumn("running_cost", F.sum("cost").over(w5))
)

result = (
    scored
    .groupBy(
        F.when(F.col("amount_share") >= 0.73, "LARGE").otherwise("SMALL").alias("amount_share_tier"),
    )
    .agg(
        F.size(F.collect_set("status")).alias("num_status"),
        F.max(F.when(F.col("status") == "RETURNED", F.col("amount"))).alias("returned_max_amount"),
        F.round(F.avg("cost_share_2"), 2).alias("avg_cost_share_2"),
        F.percentile_approx("running_cost", 0.9).alias("running_cost_p90"),
    )
)
