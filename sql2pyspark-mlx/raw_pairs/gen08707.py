from pyspark.sql import functions as F

valid = (
    df_stores
    .filter(F.col("region").isNotNull())
    .select("*", F.date_sub("opened_date", 53).alias("opened_date_minus_53d"))
)

remaining_keys = (
    valid
    .select("id", "city")
    .intersectAll(
        df_stores
        .filter(F.col("sqft").between(7089, 11593))
        .select("id", "city")
    )
)

kept_rows = (
    valid
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

eligible = (
    kept_rows
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.order_date") <= F.col("s.opened_date")), "left_semi")
)

eligible_stores = (
    eligible
    .alias("s")
    .join(df_inventory.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.quantity") <= 75), "left_anti")
)

kept = (
    eligible_stores
    .alias("s")
    .join(df_orders.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.order_date") < F.col("s.opened_date")), "left_semi")
)

result = (
    kept
    .groupBy(F.date_trunc("MONTH", "opened_date_minus_53d").alias("activity_month"))
    .agg(
        F.sum(F.when(F.col("city") == "New York", F.col("sqft")).otherwise(0)).alias("new_york_sqft"),
        F.sort_array(F.collect_set("region")).alias("region_list"),
        F.size(F.collect_set("region")).alias("num_regions"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.countDistinct("id").alias("distinct_ids"),
        F.sort_array(F.collect_set("city")).alias("city_list_2"),
    )
    .orderBy(F.col("activity_month").asc_nulls_first())
)
