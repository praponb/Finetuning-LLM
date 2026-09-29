from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("channel", "status").orderBy("discount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.delivered_date").desc(), F.col("c.id").desc())

cleaned = (
    df_orders
    .filter(F.col("order_date") >= F.lit("2023-09-17").cast("date"))
    .withColumn("order_date_quarter", F.quarter("order_date"))
)

kept = (
    cleaned
    .alias("s")
    .join(df_order_items.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.order_id").isin([15, 16, 23, 29])), "left_anti")
)

scored = (
    kept
    .filter(F.col("order_date") < "2023-07-01")
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "amount",
        F.min("discount").over(w_grp).alias("running_min_discount"),
        F.avg("discount").over(w_grp).alias("running_avg_discount"),
        F.max("discount").over(w_grp).alias("running_max_discount"),
    )
)

enriched = (
    scored
    .alias("e")
    .join(df_stores.alias("p"), F.col("e.store_id") == F.col("p.id"), "left")
    .select(
        "e.id",
        "e.customer_id",
        "e.store_id",
        "e.channel",
        "e.running_min_discount",
        "e.running_avg_discount",
        F.col("p.sqft"),
    )
)

with_last = (
    enriched
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.running_avg_discount", "s.sqft", F.col("c.carrier"))
)

result = (
    with_last
    .withColumn("running_avg_discount_tier", F.when(F.col("running_avg_discount") >= 0.36, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["running_avg_discount_tier", "carrier"], ["running_avg_discount_tier"], []],
        "running_avg_discount_tier",
        "carrier",
    )
    .agg(
        F.size(F.collect_set("carrier")).alias("num_carriers"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .filter(F.col("num_carriers") > 1)
    .orderBy(F.col("running_avg_discount_tier").desc())
)
