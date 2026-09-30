from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("unit_price").desc(), "id")

base = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select("*", F.coalesce("quantity", F.lit(0)).alias("quantity_filled"))
)

ranked = (
    base
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "order_id")
)

enriched = (
    ranked
    .alias("a")
    .join(df_orders.alias("d"), F.col("a.order_id") == F.col("d.id"), "inner")
    .select("a.*", F.col("d.channel"), F.col("d.status"))
)

valid = (
    enriched
    .filter((~F.col("status").eqNullSafe(F.lit("PENDING"))) & F.col("status").isNotNull())
    .select("*", F.substring("status", 1, 3).alias("status_prefix"))
)

result = (
    valid
    .groupBy("order_id")
    .agg(
        F.countDistinct("status").alias("distinct_status"),
        F.size(F.collect_set("status")).alias("num_status"),
        F.count("*").alias("row_count"),
        F.count_if(F.col("channel") == "web").alias("web_events"),
    )
    .filter(F.col("web_events") > 1)
    .orderBy(F.col("num_status").desc())
)
