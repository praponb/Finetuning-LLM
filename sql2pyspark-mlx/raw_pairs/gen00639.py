from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id")
w2 = Window.partitionBy("order_id").orderBy(F.col("cost_share").desc_nulls_first())
cost_by_order_id = df_shipments.groupBy("order_id").agg(F.max("cost").alias("max_shipment_cost"))

valid = (
    df_shipments
    .filter((F.col("shipped_date") >= F.to_date(F.lit("2023-02-24"))) & F.col("order_id").isNotNull())
    .withColumn("days_since_delivered_date", F.datediff(F.current_date(), "delivered_date"))
    .withColumn("resolved_carrier", F.coalesce("carrier", F.lit("N/A")))
)

with_dims = (
    valid
    .alias("s")
    .join(df_orders.alias("d"), F.col("s.order_id") == F.col("d.id"), "left")
    .select(
        "s.id",
        "s.order_id",
        "s.shipped_date",
        "s.cost",
        "s.days_since_delivered_date",
        F.col("d.order_date"),
    )
)

enriched = (
    with_dims
    .withColumn("order_id_stddev_samp_cost", F.stddev_samp("cost").over(w))
    .withColumn("cost_share", F.round((F.col("cost") / F.sum("cost").over(w)), 4))
)

deduped = (
    enriched
    .withColumn("latest_rank", F.rank().over(w2))
    .filter(F.col("latest_rank") == 1)
)

result = (
    deduped
    .join(cost_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "shipped_date",
        "cost",
        "order_date",
        "order_id_stddev_samp_cost",
        F.col("max_shipment_cost"),
    )
    .orderBy(F.col("shipped_date").desc())
)
