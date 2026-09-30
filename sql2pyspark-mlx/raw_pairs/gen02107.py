from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("region").orderBy("opened_date", "id")
w3 = Window.partitionBy("city").orderBy(F.col("opened_date").asc_nulls_first())
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.sale_date").desc(), F.col("c.id").desc())
w5 = Window.partitionBy("region").orderBy(F.col("dedup_rank").desc(), "id")
w6 = Window.partitionBy("region", "city").orderBy(F.col("inventory_quantity").desc(), "id")
w7 = Window.partitionBy("region")

with_latest = (
    df_stores
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.quantity").alias("inventory_quantity"),
        F.col("c.last_restock"),
    )
)

latest = (
    with_latest
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
)

latest_stores = (
    latest
    .withColumn("latest_rank", F.rank().over(w3))
    .filter(F.col("latest_rank") == 1)
)

with_latest_stores = (
    latest_stores
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.sale_date") <= F.col("s.opened_date")), "inner")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.sale_date").alias("store_sale_sale_date"))
)

enriched = (
    with_latest_stores
    .select(
        "id",
        "store_name",
        "region",
        "opened_date",
        F.row_number().over(w5).alias("row_seq"),
        F.ntile(3).over(w6).alias("inventory_quantity_bucket"),
        F.round((F.col("sqft") / F.sum("sqft").over(w7)), 4).alias("sqft_share"),
    )
)

kept = (
    enriched
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.order_date") >= F.col("s.opened_date")), "left_anti")
)

totals = (
    kept
    .groupBy(F.date_trunc("WEEK", "opened_date").alias("week_start"))
    .agg(
        F.count_if(F.col("region") == "North").alias("north_events"),
        F.count(F.when(F.col("region") == "West", 1)).alias("west_count"),
        F.round(F.sum("inventory_quantity_bucket") / F.nullif(F.count(F.when(F.col("region") == "East", 1)), F.lit(0)), 2).alias("inventory_quantity_bucket_per_east"),
        F.countDistinct("id").alias("distinct_ids"),
        F.sort_array(F.collect_set("region")).alias("region_list"),
        F.round(F.sum("inventory_quantity_bucket") / F.nullif(F.count(F.when(F.col("region") == "West", 1)), F.lit(0)), 2).alias("inventory_quantity_bucket_per_west"),
    )
    .filter(F.col("north_events") < 1000)
)

result = (
    totals
    .select(
        "week_start",
        "north_events",
        "west_count",
        "inventory_quantity_bucket_per_east",
        "distinct_ids",
        "region_list",
        "inventory_quantity_bucket_per_west",
    )
    .orderBy(F.col("inventory_quantity_bucket_per_east").asc_nulls_last())
)
