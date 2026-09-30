from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("order_id").orderBy("cost")
w2 = Window.partitionBy(F.date_trunc("week", "order_date"))
w3 = Window.orderBy("cost")
w4 = Window.partitionBy("order_id")

filtered = (
    df_shipments
    .filter((F.col("delivered_date") > F.lit("2023-08-05").cast("date")) & F.col("order_id").isNotNull())
    .select("*", F.upper("status").alias("status_upper"))
)

remaining_keys = (
    filtered
    .select("status", "id")
    .intersectAll(
        df_shipments
        .filter(~F.col("status").isin(["DELIVERED", "RETURNED"]))
        .select("status", "id")
    )
)

kept_rows = (
    filtered
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched = (
    kept_rows
    .alias("a")
    .join(df_orders.alias("p"), F.col("a.order_id").eqNullSafe(F.col("p.id")), "left")
    .select(
        "a.id",
        "a.order_id",
        "a.carrier",
        "a.shipped_date",
        "a.cost",
        "a.status",
        F.col("p.order_date"),
    )
)

metrics = (
    enriched
    .withColumn("cost_dense_rank", F.dense_rank().over(w_part))
    .withColumn("cost_share", F.round((F.col("cost") / F.sum("cost").over(w2)), 4))
    .withColumn("cost_dense_rank_2", F.dense_rank().over(w3))
    .withColumn("order_id_max_cost", F.max("cost").over(w4))
)

cleaned = (
    metrics
    .filter(F.col("order_id").isNotNull())
    .select("*", F.upper("status").alias("status_upper"))
)

result = (
    cleaned
    .select(
        "id",
        "carrier",
        "shipped_date",
        "cost",
        "order_date",
        "cost_dense_rank",
        "order_id_max_cost",
        "status_upper",
        F.when(F.col("carrier").rlike("^(Kerry|FedEx)$"), "PRIORITY").otherwise("CORE").alias("carrier_zone"),
    )
    .orderBy("cost")
)
