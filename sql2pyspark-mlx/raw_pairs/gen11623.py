from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())

valid = (
    df_stores
    .filter(F.col("sqft") == 12476)
    .withColumn("region_length", F.length("region"))
)

with_latest = (
    valid
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.store_name",
        "s.city",
        "s.region",
        "s.opened_date",
        "s.region_length",
        F.col("c.order_date"),
    )
)

kept_keys = (
    with_latest
    .select("region", "city")
    .subtract(
        df_stores
        .filter(F.col("sqft") >= 15201)
        .select("region", "city")
    )
)

verified = (
    with_latest
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.region") == F.col("k.region")) & (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

kept_keys_2 = (
    verified
    .select("region", "city")
    .subtract(
        df_stores
        .filter(F.col("sqft") < 15684)
        .select("region", "city")
    )
)

verified_stores = (
    verified
    .alias("s")
    .join(kept_keys_2.alias("k"), (F.col("s.region") == F.col("k.region")) & (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

kept = (
    verified_stores
    .alias("s")
    .join(df_inventory.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.last_restock") <= F.col("s.order_date")), "left_anti")
)

result = (
    kept
    .withColumn("activity_month", F.date_trunc("month", "order_date"))
    .rollup("activity_month", "region")
    .agg(
        F.sum("region_length").alias("total_region_length"),
        F.sort_array(F.collect_set("region")).alias("region_list"),
        F.sum(F.when(F.col("city") == "Tokyo", F.col("region_length")).otherwise(0)).alias("tokyo_region_length"),
        F.grouping_id().alias("gid"),
    )
    .orderBy(F.col("activity_month").desc())
)
