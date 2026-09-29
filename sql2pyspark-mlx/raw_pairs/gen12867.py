from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("opened_date"))
w2 = Window.partitionBy("region").orderBy("opened_date", "id")
w3 = Window.partitionBy("city")
w_recent = Window.partitionBy("city", "region").orderBy("opened_date_month_avg_sqft", "id").rowsBetween(-6, Window.currentRow)
w5 = Window.partitionBy("city").orderBy(F.col("next_sqft").asc_nulls_first())
w6 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.shipping_fee").desc(), F.col("c.id").desc())
w_running = Window.partitionBy("city").orderBy("next_sqft", "id")
w8 = Window.partitionBy("region")

metrics = (
    df_stores
    .withColumn("opened_date_month_avg_sqft", F.avg("sqft").over(w))
    .withColumn("next_sqft", F.lead("sqft", 2).over(w2))
    .withColumn("city_max_sqft", F.max("sqft").over(w3))
)

valid = (
    metrics
    .filter(F.col("opened_date") > F.lit("2023-06-12").cast("date"))
    .select(
        "id",
        "city",
        "region",
        "opened_date",
        "opened_date_month_avg_sqft",
        "next_sqft",
        "city_max_sqft",
        F.concat_ws("/", "store_name", "city").alias("store_name_city_key"),
    )
)

eligible = (
    valid
    .alias("s")
    .join(df_orders.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.order_date") < F.col("s.opened_date")), "left_semi")
)

enriched = (
    eligible
    .select(
        "*",
        F.sum("city_max_sqft").over(w_recent).alias("last7_sum_city_max_sqft"),
        F.avg("city_max_sqft").over(w_recent).alias("last7_avg_city_max_sqft"),
        F.min("city_max_sqft").over(w_recent).alias("last7_min_city_max_sqft"),
        F.cume_dist().over(w5).alias("next_sqft_cume_dist"),
    )
)

with_latest = (
    enriched
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.amount").alias("order_amount"))
)

not_flagged = (
    with_latest
    .alias("s")
    .join(df_inventory.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.last_restock") < F.col("s.opened_date")), "left_semi")
)

with_windows = (
    not_flagged
    .select(
        "id",
        "city",
        "region",
        "opened_date_month_avg_sqft",
        "city_max_sqft",
        "last7_avg_city_max_sqft",
        "last7_min_city_max_sqft",
        F.lead("last7_min_city_max_sqft").over(w_running).alias("next_last7_min_city_max_sqft"),
        F.lag("last7_min_city_max_sqft").over(w_running).alias("prev_last7_min_city_max_sqft"),
        F.lead("next_sqft").over(w_running).alias("next_next_sqft"),
        F.row_number().over(w_running).alias("event_seq"),
        F.min("last7_min_city_max_sqft").over(w8).alias("region_min_last7_min_city_max_sqft"),
    )
)

stats = (
    with_windows
    .groupBy("region")
    .agg(
        F.count_if(F.col("city") == "London").alias("london_events"),
        F.countDistinct("region").alias("distinct_regions"),
        F.min("opened_date_month_avg_sqft").alias("min_opened_date_month_avg_sqft"),
        F.count(F.when(F.col("region") == "North", 1)).alias("north_rows"),
        F.count(F.when(F.col("city") == "Singapore", 1)).alias("singapore_count"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
    )
)

result = stats.select("region", "min_opened_date_month_avg_sqft", "singapore_count")
