from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("opened_date").desc(), "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.sale_date").desc(), F.col("c.id").desc())
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())

base = (
    df_stores
    .filter(F.col("region").isNotNull() & (F.col("opened_date") > F.to_date(F.lit("2023-05-30"))))
    .select(
        "id",
        "store_name",
        "city",
        "opened_date",
        F.datediff(F.current_date(), "opened_date").alias("days_since_opened_date"),
        F.trim("store_name").alias("clean_store_name"),
        F.coalesce("region", F.lit("OTHER")).alias("resolved_region"),
    )
)

deduped = (
    base
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

with_last = (
    deduped
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.revenue").alias("store_sale_revenue"),
        F.col("c.sale_date").alias("store_sale_sale_date"),
    )
)

kept = (
    with_last
    .alias("s")
    .join(df_inventory.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.product_id") == 7), "left_anti")
)

latest_match = (
    kept
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.days_since_opened_date",
        "s.clean_store_name",
        "s.dedup_rank",
        "s.store_sale_revenue",
        "s.store_sale_sale_date",
        F.col("c.quantity").alias("inventory_quantity"),
        F.col("c.last_restock").alias("inventory_last_restock"),
    )
)

result = (
    latest_match
    .groupBy(
        F.when(F.col("inventory_quantity") >= 243, "LARGE").otherwise("SMALL").alias("inventory_quantity_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("id").alias("distinct_ids_2"),
        F.percentile_approx("inventory_quantity", 0.5).alias("inventory_quantity_p50"),
        F.count("*").alias("event_count"),
        F.round(F.avg("store_sale_revenue"), 2).alias("avg_store_sale_revenue"),
    )
    .orderBy("event_count")
)
