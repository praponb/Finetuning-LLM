from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("sqft").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.order_date").desc(), F.col("c.id").desc())

base = (
    df_stores
    .filter((F.col("opened_date") >= F.lit("2023-06-10").cast("date")) & F.col("region").isNotNull())
    .withColumn("region_prefix", F.substring("region", 1, 2))
    .withColumn("region_clean", F.coalesce("region", F.lit("NONE")))
)

deduped = (
    base
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
)

latest_match = (
    deduped
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.store_name",
        "s.city",
        "s.region",
        "s.opened_date",
        "s.sqft",
        "s.region_prefix",
        "s.region_clean",
        F.col("c.last_restock").alias("inventory_last_restock"),
        F.col("c.quantity"),
    )
)

with_last = (
    latest_match
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.order_date") <= F.col("s.inventory_last_restock")), "inner")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.region",
        "s.region_prefix",
        "s.region_clean",
        "s.inventory_last_restock",
        F.col("c.discount"),
    )
)

summary = (
    with_last
    .withColumn("discount_tier", F.when(F.col("discount") >= 0.37, "HIGH").otherwise("LOW"))
    .rollup("discount_tier")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("event_count"),
        F.countDistinct("id").alias("distinct_ids_2"),
        F.count("*").alias("row_count"),
        F.grouping("discount_tier").alias("grouping_level"),
    )
    .filter(F.col("event_count") < 1000)
)

result = (
    summary
    .select("distinct_ids", "event_count", "grouping_level")
    .orderBy(F.col("grouping_level").asc_nulls_first())
)
