from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w_part = Window.partitionBy("region").orderBy("quantity", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.partitionBy("region").orderBy(F.col("last4_avg_opened_date_month").desc_nulls_first(), "id")
w4 = Window.partitionBy("region").orderBy("last4_avg_opened_date_month", "id")
w5 = Window.partitionBy("region").orderBy("last4_avg_opened_date_month", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_latest = (
    df_stores
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.quantity"),
        F.col("c.last_restock").alias("inventory_last_restock"),
    )
)

cleaned = (
    with_latest
    .filter(F.col("region").isNotNull() & (F.col("opened_date") <= "2025-08-01"))
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "quantity",
        "inventory_last_restock",
        F.month("opened_date").alias("opened_date_month"),
    )
)

eligible = (
    cleaned
    .alias("s")
    .join(df_orders.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.order_date") > "2024-03-15"), "left_semi")
)

metrics = (
    eligible
    .select(
        "id",
        "store_name",
        "region",
        "inventory_last_restock",
        F.max("opened_date_month").over(w_part).alias("last4_max_opened_date_month"),
        F.sum("opened_date_month").over(w_part).alias("last4_sum_opened_date_month"),
        F.avg("opened_date_month").over(w_part).alias("last4_avg_opened_date_month"),
    )
)

kept = (
    metrics
    .alias("s")
    .join(df_store_sales.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.sale_date") >= F.col("s.inventory_last_restock")), "left_semi")
)

ranked = (
    kept
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

enriched = (
    ranked
    .withColumn("next_last4_sum_opened_date_month", F.lead("last4_sum_opened_date_month", 2).over(w4))
    .withColumn("running_last4_avg_opened_date_month", F.sum("last4_avg_opened_date_month").over(w5))
)

result = (
    enriched
    .select(
        "id",
        "store_name",
        "region",
        "last4_max_opened_date_month",
        "last4_sum_opened_date_month",
        "last4_avg_opened_date_month",
        "next_last4_sum_opened_date_month",
        "running_last4_avg_opened_date_month",
    )
    .orderBy(
        F.col("last4_avg_opened_date_month").desc(),
        "running_last4_avg_opened_date_month",
    )
)
