from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.discount").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("region").orderBy(F.col("opened_date").desc_nulls_last(), "id")
w3 = Window.partitionBy("city").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("city").orderBy("order_shipping_fee", "id")

not_flagged = (
    df_stores
    .alias("s")
    .join(df_inventory.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.last_restock") < F.col("s.opened_date")), "left_anti")
)

eligible = (
    not_flagged
    .alias("s")
    .join(df_store_sales.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.sale_date") < F.col("s.opened_date")), "left_semi")
)

with_latest = (
    eligible
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.shipping_fee").alias("order_shipping_fee"),
        F.col("c.amount"),
    )
)

deduped = (
    with_latest
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
)

metrics = (
    deduped
    .filter(F.col("store_name").isNotNull())
    .select(
        "id",
        "city",
        "region",
        "amount",
        F.max("order_shipping_fee").over(w3).alias("running_max_order_shipping_fee"),
        F.count("order_shipping_fee").over(w3).alias("running_count_order_shipping_fee"),
        F.sum("order_shipping_fee").over(w3).alias("running_sum_order_shipping_fee"),
        F.lead("order_shipping_fee", 1).over(w4).alias("next_order_shipping_fee"),
    )
)

grouped = (
    metrics
    .groupBy("region")
    .agg(
        F.round(F.sum("amount") / F.nullif(F.count(F.when(F.col("city") == "London", 1)), F.lit(0)), 2).alias("amount_per_london"),
        F.round(F.avg("running_count_order_shipping_fee"), 2).alias("avg_running_count_order_shipping_fee"),
        F.min("amount").alias("min_amount"),
        F.sum("next_order_shipping_fee").alias("total_next_order_shipping_fee"),
    )
)

result = (
    grouped
    .select(
        "region",
        "amount_per_london",
        "avg_running_count_order_shipping_fee",
        "total_next_order_shipping_fee",
        F.when(F.col("region").rlike("^(North|South)$"), "EXTENDED").when(F.col("region").rlike("^(East|West)$"), "CORE").otherwise("OTHER_ZONE").alias("region_zone"),
    )
    .orderBy("avg_running_count_order_shipping_fee")
)
