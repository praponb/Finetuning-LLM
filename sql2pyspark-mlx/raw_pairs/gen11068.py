from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "carrier").orderBy(F.col("delivered_date").asc_nulls_first())
w2 = Window.partitionBy("order_id").orderBy(F.col("cost").desc_nulls_first())
w3 = Window.orderBy("order_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("order_id").orderBy("order_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("order_id")
w7 = Window.partitionBy("order_id").orderBy(F.col("amount").desc(), "id")

latest = (
    df_shipments
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "order_id", "delivered_date", "cost")
)

enriched = (
    latest
    .alias("src")
    .join(df_orders.alias("d"), F.col("src.order_id") == F.col("d.id"), "inner")
    .filter(F.col("d.channel").isin(["partner", "web"]))
    .select(
        "src.id",
        "src.order_id",
        "src.delivered_date",
        "src.cost",
        F.col("d.amount"),
        F.col("d.order_date"),
    )
)

scored = (
    enriched
    .withColumn("cost_dense_rank", F.dense_rank().over(w2))
    .withColumn("running_amount", F.sum("amount").over(w3))
    .withColumn("running_cost", F.sum("cost").over(w4))
    .withColumn("first_known_amount", F.first("amount", ignorenulls=True).over(w5))
    .withColumn("order_id_stddev_samp_cost", F.stddev_samp("cost").over(w6))
)

base = (
    scored
    .filter((F.col("delivered_date") >= F.to_date(F.lit("2023-09-10"))) & F.col("order_id").isNotNull())
    .withColumn("running_amount_tier", F.when(F.col("running_amount") >= 8518, "High").when(F.col("running_amount") >= 1749, "Medium").otherwise("Low"))
    .withColumn("cost_plus_order_id_stddev_samp_cost", (F.col("cost") + F.col("order_id_stddev_samp_cost")))
)

metrics = (
    base
    .withColumn("order_id_min_amount", F.min("amount").over(w6))
    .withColumn("event_seq", F.row_number().over(w7))
)

result = (
    metrics
    .select(
        "id",
        "cost",
        "amount",
        "cost_dense_rank",
        "running_amount",
        "first_known_amount",
        "running_amount_tier",
        "cost_plus_order_id_stddev_samp_cost",
        "order_id_min_amount",
    )
    .orderBy(F.col("order_id_min_amount").desc())
)
