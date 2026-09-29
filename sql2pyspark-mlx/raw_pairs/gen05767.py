from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("status")
w2 = Window.partitionBy("status", "order_id").orderBy(F.col("amount").desc())
w3 = Window.partitionBy("status", "order_id")
cost_by_order_id = df_shipments.groupBy("order_id").agg(F.max("cost").alias("max_shipment_cost"))

filtered = (
    df_shipments
    .filter(F.col("order_id").isNotNull())
    .select(
        "*",
        F.col("cost").cast("int").alias("cost_int"),
        F.col("cost").cast("int").alias("cost_int_2"),
    )
)

kept_keys = (
    filtered
    .select("id", "carrier")
    .intersect(
        df_shipments
        .filter(F.col("carrier").isin(["DHL", "Kerry"]))
        .select("id", "carrier")
    )
)

verified = (
    filtered
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.carrier") == F.col("k.carrier")), "inner")
    .select("s.*")
)

with_dims = (
    verified
    .alias("a")
    .join(df_orders.alias("d"), F.col("a.order_id") == F.col("d.id"), "inner")
    .filter(F.col("d.status") == "COMPLETED")
    .select(
        "a.order_id",
        "a.delivered_date",
        "a.cost",
        "a.status",
        "a.cost_int",
        "a.cost_int_2",
        F.col("d.amount"),
        F.col("d.order_date"),
    )
)

scored = (
    with_dims
    .select(
        "order_id",
        "delivered_date",
        "cost",
        "status",
        "amount",
        F.max("cost_int").over(w_ordered).alias("status_max_cost_int"),
    )
)

joined = (
    scored
    .alias("s")
    .join(df_orders.alias("b"), F.col("s.order_id").eqNullSafe(F.col("b.id")), "left")
    .select(
        "s.order_id",
        "s.delivered_date",
        "s.status",
        "s.amount",
        "s.status_max_cost_int",
        F.col("b.discount"),
        F.col("b.order_date"),
    )
)

scored_shipments = (
    joined
    .withColumn("amount_percent_rank", F.percent_rank().over(w2))
    .withColumn("status_min_discount", F.min("discount").over(w3))
)

result = (
    scored_shipments
    .join(cost_by_order_id, on="order_id", how="left")
    .select(
        "order_id",
        "delivered_date",
        "status",
        "amount",
        "discount",
        "order_date",
        F.when(F.col("status").rlike("^(IN_TRANSIT|RETURNED)$"), "GROUP_A").when(F.col("status").rlike("^(DELIVERED|LOST)$"), "OTHER_ZONE").otherwise("EXTENDED").alias("status_zone"),
        F.col("max_shipment_cost"),
    )
    .orderBy(F.col("status").desc())
)
