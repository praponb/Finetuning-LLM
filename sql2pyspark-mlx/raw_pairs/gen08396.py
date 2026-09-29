from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("amount").desc_nulls_first(), "id")

deduped = (
    df_orders
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select(
        "id",
        "customer_id",
        "store_id",
        "status",
        "amount",
        "discount",
        "shipping_fee",
    )
)

not_flagged = (
    deduped
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.unit_price").between(180, 390)), "left_anti")
)

kept_keys = (
    not_flagged
    .select("status", "customer_id")
    .subtract(
        df_orders
        .filter(F.col("shipping_fee") < 15)
        .select("status", "customer_id")
    )
)

reconciled = (
    not_flagged
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

with_dims = (
    reconciled
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.segment") != "Consumer")
    .select(
        "src.id",
        "src.customer_id",
        "src.store_id",
        "src.status",
        "src.discount",
        F.col("c.email"),
        F.col("c.country"),
    )
)

result = (
    with_dims
    .alias("s")
    .join(df_stores.alias("d"), F.col("s.store_id") == F.col("d.id"), "inner")
    .rollup(
        F.col("d.region").alias("region"),
        F.col("d.city").alias("city"),
        F.col("s.country").alias("country"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.avg(F.col("s.discount")), 2).alias("avg_discount"),
        F.grouping_id().alias("gid"),
    )
)
