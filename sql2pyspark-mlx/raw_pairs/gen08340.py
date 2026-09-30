from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region")
w_running = Window.orderBy("sqft", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.orderBy(F.col("sqft").desc(), "id")
w4 = Window.partitionBy(F.date_trunc("week", "opened_date"), "city").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.sale_date").desc(), F.col("c.id").desc())
w6 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.shipping_fee").desc(), F.col("c.id").desc())
w7 = Window.partitionBy("city").orderBy(F.col("sale_date").desc(), "id")
w8 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w9 = Window.partitionBy("city").orderBy(F.col("row_num").desc(), "id")
w10 = Window.partitionBy("city").orderBy("quantity", "id")

enriched = (
    df_stores
    .filter(F.col("store_name").like("M%"))
    .select(
        "id",
        "city",
        F.round((F.col("sqft") / F.sum("sqft").over(w)), 4).alias("sqft_share"),
        F.avg("sqft").over(w_running).alias("sqft_avg_3"),
        F.round((F.col("sqft") / F.sum("sqft").over(w)), 4).alias("sqft_share_2"),
        F.ntile(4).over(w3).alias("sqft_bucket"),
        F.last("region", ignorenulls=True).over(w4).alias("last_known_region"),
    )
)

with_latest = (
    enriched
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.sale_date"))
)

latest_match = (
    with_latest
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.shipping_fee"))
)

latest = (
    latest_match
    .withColumn("row_num", F.row_number().over(w7))
    .filter(F.col("row_num") == 1)
)

with_latest_stores = (
    latest
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w8))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.quantity"),
        F.col("c.last_restock").alias("inventory_last_restock"),
    )
)

scored = (
    with_latest_stores
    .select(
        "id",
        "city",
        "sqft_bucket",
        "last_known_region",
        "row_num",
        "quantity",
        "inventory_last_restock",
        F.row_number().over(w9).alias("seq_no"),
        F.lag("sqft_avg_3", 2).over(w10).alias("prev_sqft_avg_3"),
    )
)

result = (
    scored
    .groupBy(
        F.when(F.col("quantity") >= 214, "HIGH").otherwise("LOW").alias("quantity_tier"),
        F.when(F.col("seq_no") >= 8, "HIGH").otherwise("LOW").alias("seq_no_tier"),
    )
    .agg(
        F.count_if(F.col("city") == "Chiang Mai").alias("chiang_mai_events"),
        F.count(F.when(F.col("city") == "Chiang Mai", 1)).alias("chiang_mai_count"),
        F.sum("prev_sqft_avg_3").alias("total_prev_sqft_avg_3"),
        F.size(F.collect_set("city")).alias("num_cities"),
        F.countDistinct("city").alias("distinct_cities"),
        F.count_if(F.col("city") == "New York").alias("new_york_events"),
    )
    .orderBy(F.col("new_york_events").desc())
)
