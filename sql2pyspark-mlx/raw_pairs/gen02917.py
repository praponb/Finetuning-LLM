from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.sale_date").desc(), F.col("c.id").desc())

base = (
    df_stores
    .filter(F.col("region").isNotNull() & (F.col("region") == "North"))
    .withColumn("store_name_lower", F.lower("store_name"))
)

with_latest = (
    base
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.store_name",
        "s.city",
        "s.region",
        F.col("c.last_restock").alias("inventory_last_restock"),
        F.col("c.quantity").alias("inventory_quantity"),
    )
)

with_last = (
    with_latest
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.sale_date") <= F.col("s.inventory_last_restock")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.store_name",
        "s.inventory_last_restock",
        "s.inventory_quantity",
        F.col("c.sale_date").alias("store_sale_sale_date"),
        F.col("c.revenue").alias("store_sale_revenue"),
    )
)

not_flagged = (
    with_last
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.quantity") >= 81), "left_semi")
)

cleaned = (
    not_flagged
    .filter(F.col("store_sale_sale_date").between("2024-05-29", "2025-06-16"))
    .select(
        "*",
        (F.col("inventory_quantity") + F.col("store_sale_revenue")).alias("inventory_quantity_plus_store_sale_revenue"),
        F.round((F.col("store_sale_revenue") * 100), 2).alias("adjusted_store_sale_revenue"),
    )
)

eligible = (
    cleaned
    .alias("s")
    .join(df_store_sales.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.sale_date") < F.col("s.store_sale_sale_date")), "left_anti")
)

eligible_stores = (
    eligible
    .alias("s")
    .join(df_orders.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.order_date") < F.col("s.inventory_last_restock")), "left_anti")
)

result = (
    eligible_stores
    .groupBy(F.date_trunc("MONTH", "inventory_last_restock").alias("activity_month"))
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.sum("inventory_quantity").alias("total_inventory_quantity"),
        F.countDistinct("id").alias("distinct_ids_2"),
        F.percentile_approx("inventory_quantity_plus_store_sale_revenue", 0.5).alias("inventory_quantity_plus_store_sale_revenue_p50"),
        F.count("*").alias("event_count"),
    )
    .orderBy("distinct_ids")
)
