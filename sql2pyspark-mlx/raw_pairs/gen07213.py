from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.discount").desc(), F.col("c.id").desc())
w2 = Window.orderBy(F.col("shipping_fee").asc_nulls_last())
w3 = Window.partitionBy("region")

kept = (
    df_stores
    .alias("s")
    .join(df_store_sales.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.sale_date") < F.col("s.opened_date")), "left_semi")
)

eligible = (
    kept
    .alias("s")
    .join(df_orders.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.order_date") <= F.col("s.opened_date")), "left_anti")
)

latest_match = (
    eligible
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.store_name", "s.region", "s.sqft", F.col("c.shipping_fee"))
)

remaining_keys = (
    latest_match
    .select("region", "id")
    .intersectAll(
        df_stores
        .filter(F.col("sqft") != 13134)
        .select("region", "id")
    )
)

kept_rows = (
    latest_match
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.region") == F.col("k.region")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

metrics = (
    kept_rows
    .select(
        "id",
        "region",
        "shipping_fee",
        F.cume_dist().over(w2).alias("shipping_fee_cume_dist"),
        F.round((F.col("sqft") / F.sum("sqft").over(w3)), 4).alias("sqft_share"),
    )
)

eligible_stores = (
    metrics
    .alias("s")
    .join(df_inventory.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.quantity") < 127), "left_semi")
)

result = (
    eligible_stores
    .select(
        "region",
        "shipping_fee",
        "shipping_fee_cume_dist",
        "sqft_share",
        F.when(F.col("region").rlike("^(Central|East)$"), "CORE").when(F.col("region").rlike("^(North|South)$"), "EXTENDED").otherwise("OTHER_ZONE").alias("region_zone"),
    )
    .orderBy(F.col("region").desc_nulls_first())
)
