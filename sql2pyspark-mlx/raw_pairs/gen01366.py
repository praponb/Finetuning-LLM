from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w_recent = Window.partitionBy("region", "city").orderBy("inventory_quantity", "id").rowsBetween(-6, Window.currentRow)
w3 = Window.orderBy(F.col("inventory_quantity").desc(), "id")
w4 = Window.partitionBy("city", "region").orderBy("last7_avg_inventory_quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("city").orderBy("running_sum_last7_sum_inventory_quantity", "id")
w6 = Window.orderBy("running_max_last7_sum_inventory_quantity", "id").rowsBetween(-5, -1)

cleaned = (
    df_stores
    .filter(F.col("region").isNotNull())
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "sqft",
        F.regexp_replace("store_name", " ", "").alias("store_name_slug"),
        F.coalesce("region", F.lit("NONE")).alias("resolved_region"),
    )
)

with_last = (
    cleaned
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.quantity").alias("inventory_quantity"))
)

scored = (
    with_last
    .select(
        "*",
        F.sum("inventory_quantity").over(w_recent).alias("last7_sum_inventory_quantity"),
        F.min("inventory_quantity").over(w_recent).alias("last7_min_inventory_quantity"),
        F.count("inventory_quantity").over(w_recent).alias("last7_count_inventory_quantity"),
        F.avg("inventory_quantity").over(w_recent).alias("last7_avg_inventory_quantity"),
        F.row_number().over(w3).alias("seq_no"),
    )
)

scored_stores = (
    scored
    .filter(F.col("region").isin(["North", "West", "Central"]))
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "sqft",
        "inventory_quantity",
        "seq_no",
        F.max("last7_sum_inventory_quantity").over(w4).alias("running_max_last7_sum_inventory_quantity"),
        F.sum("last7_sum_inventory_quantity").over(w4).alias("running_sum_last7_sum_inventory_quantity"),
        F.min("last7_sum_inventory_quantity").over(w4).alias("running_min_last7_sum_inventory_quantity"),
    )
)

enriched = (
    scored_stores
    .select(
        "*",
        F.lead("running_sum_last7_sum_inventory_quantity").over(w_running).alias("next_running_sum_last7_sum_inventory_quantity"),
        F.lag("running_sum_last7_sum_inventory_quantity").over(w_running).alias("prev_running_sum_last7_sum_inventory_quantity"),
        F.lead("running_max_last7_sum_inventory_quantity").over(w_running).alias("next_running_max_last7_sum_inventory_quantity"),
        F.row_number().over(w_running).alias("row_seq"),
        F.avg("running_sum_last7_sum_inventory_quantity").over(w6).alias("running_sum_last7_sum_inventory_quantity_avg_5_prior"),
    )
)

eligible = (
    enriched
    .alias("s")
    .join(df_store_sales.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.units") < 18), "left_semi")
)

result = (
    eligible
    .groupBy("city")
    .agg(
        F.bit_or("sqft").alias("sqft_bit_or"),
        F.countDistinct("city").alias("distinct_cities"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.size(F.collect_set("city")).alias("num_cities"),
        F.round(F.avg("next_running_max_last7_sum_inventory_quantity"), 2).alias("avg_next_running_max_last7_sum_inventory_quantity"),
    )
    .orderBy(F.col("avg_next_running_max_last7_sum_inventory_quantity").desc())
)
