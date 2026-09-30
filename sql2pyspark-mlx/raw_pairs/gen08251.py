from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.order_date").desc(), F.col("c.id").desc())

with_latest = (
    df_stores
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.order_date") <= F.col("s.opened_date")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.store_name",
        "s.city",
        "s.sqft",
        F.col("c.status").alias("order_status"),
    )
)

remaining_keys = (
    with_latest
    .select("city", "id")
    .intersect(
        df_stores
        .filter(F.col("opened_date") >= "2023-06-05")
        .select("city", "id")
    )
)

kept_rows = (
    with_latest
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.city") == F.col("k.city")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

remaining_keys_2 = (
    kept_rows
    .select("id", "city")
    .subtract(
        df_stores
        .filter(F.col("city") != "Berlin")
        .select("id", "city")
    )
)

reconciled = (
    kept_rows
    .alias("s")
    .join(remaining_keys_2.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

eligible = (
    reconciled
    .alias("s")
    .join(df_inventory.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.quantity").between(173, 231)), "left_anti")
)

not_flagged = (
    eligible
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.quantity").between(174, 201)), "left_anti")
)

result = (
    not_flagged
    .withColumn("sqft_tier", F.when(F.col("sqft") >= 16458, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["sqft_tier", "order_status"], ["sqft_tier"]],
        "sqft_tier",
        "order_status",
    )
    .agg(
        F.count_if(F.col("order_status") == "RETURNED").alias("returned_events"),
        F.sort_array(F.collect_set("order_status")).alias("order_status_list"),
        F.count_if(F.col("order_status") == "PENDING").alias("pending_events"),
        F.sum(F.when(F.col("order_status") == "PENDING", F.col("sqft")).otherwise(0)).alias("pending_sqft"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
    )
    .orderBy("returned_events")
)
