from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.order_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("city", "region").orderBy(F.col("last_restock").asc_nulls_last(), "id")
w4 = Window.partitionBy("city").orderBy(F.col("dedup_rank").desc())

cleaned = (
    df_stores
    .filter(F.col("region").isNotNull())
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "sqft",
        F.trim("store_name").alias("clean_store_name"),
        F.coalesce("region", F.lit("N/A")).alias("region_filled"),
    )
)

with_last = (
    cleaned
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.store_name",
        "s.city",
        "s.region",
        "s.opened_date",
        "s.sqft",
        "s.clean_store_name",
        "s.region_filled",
        F.col("c.shipping_fee"),
    )
)

latest_match = (
    with_last
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.last_restock") <= F.col("s.opened_date")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.quantity").alias("inventory_quantity"),
        F.col("c.last_restock"),
    )
)

deduped = (
    latest_match
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
)

eligible = (
    deduped
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.amount") < 430), "left_semi")
)

ranked = (
    eligible
    .withColumn("row_num", F.rank().over(w4))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

wide = (
    ranked
    .groupBy("city")
    .pivot("region", ["South", "North"])
    .agg(F.sum("sqft").alias("amount"), F.count(F.lit(1)).alias("n"))
)

result = (
    wide
    .select(
        "city",
        "South_amount",
        "North_amount",
        "North_n",
        F.when(F.col("city").rlike("^(Tokyo|New York)$"), "GROUP_B").when(F.col("city").rlike("^(Bangkok|Chiang Mai)$"), "EXTENDED").otherwise("PRIORITY").alias("city_zone"),
    )
    .orderBy("South_amount")
)
