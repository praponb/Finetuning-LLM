from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.sale_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.shipping_fee").desc(), F.col("c.id").desc())

with_last = (
    df_stores
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.units").alias("store_sale_units"), F.col("c.sale_date"))
)

not_flagged = (
    with_last
    .alias("s")
    .join(df_orders.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.order_date") >= F.col("s.sale_date")), "left_semi")
)

not_flagged_stores = (
    not_flagged
    .alias("s")
    .join(df_orders.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.discount").isNull()), "left_semi")
)

with_last_stores = (
    not_flagged_stores
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.city",
        "s.region",
        "s.opened_date",
        "s.sqft",
        "s.store_sale_units",
        F.col("c.amount"),
    )
)

kept_keys = (
    with_last_stores
    .select("id", "city")
    .subtract(
        df_stores
        .filter(F.col("sqft") >= 13866)
        .select("id", "city")
    )
)

verified = (
    with_last_stores
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

kept_keys_2 = (
    verified
    .select("city", "region")
    .intersectAll(
        df_stores
        .filter(F.col("sqft") <= 5643)
        .select("city", "region")
    )
)

reconciled = (
    verified
    .alias("s")
    .join(kept_keys_2.alias("k"), (F.col("s.city") == F.col("k.city")) & (F.col("s.region") == F.col("k.region")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy("city")
    .pivot("region", ["South", "Central", "North"])
    .agg(F.sum("amount"))
)
