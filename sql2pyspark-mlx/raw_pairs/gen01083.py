from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("opened_date"), "store_id").orderBy(F.col("sqft").desc(), "id")
w2 = Window.partitionBy("store_id").orderBy("quantity", "id").rowsBetween(-6, -1)
w3 = Window.partitionBy("store_id", "region").orderBy("sqft_avg_6_prior", "id").rowsBetween(-2, -1)
w4 = Window.partitionBy("region").orderBy("sqft_avg_6_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("store_id").orderBy(F.col("sqft_bucket").desc(), "id")
w_running = Window.partitionBy("store_id", "region").orderBy("sqft_bucket", "id")
w7 = Window.partitionBy("store_id").orderBy("sqft_avg_6_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy("sqft", "id").rowsBetween(-6, Window.currentRow)

filtered = (
    df_inventory
    .filter(F.col("quantity").between(104, 218))
    .select(
        "id",
        "store_id",
        "quantity",
        "last_restock",
        F.add_months("last_restock", 1).alias("last_restock_plus_months"),
    )
)

enriched = (
    filtered
    .alias("a")
    .join(df_stores.alias("p"), F.col("a.store_id") == F.col("p.id"), "left")
    .select(
        "a.id",
        "a.store_id",
        "a.quantity",
        "a.last_restock_plus_months",
        F.col("p.sqft"),
        F.col("p.region"),
        F.col("p.opened_date"),
    )
)

metrics = (
    enriched
    .filter(F.col("opened_date") <= "2024-10-01")
    .select(
        "id",
        "store_id",
        "sqft",
        "region",
        F.ntile(3).over(w).alias("sqft_bucket"),
        F.avg("sqft").over(w2).alias("sqft_avg_6_prior"),
    )
)

metrics_inventory = (
    metrics
    .select(
        "id",
        "sqft",
        "sqft_avg_6_prior",
        F.avg("sqft").over(w3).alias("sqft_avg_2_prior"),
        F.sum("sqft").over(w4).alias("running_sqft"),
        F.ntile(10).over(w5).alias("sqft_bucket_bucket"),
        F.lead("sqft", 2, 0).over(w_running).alias("next_sqft"),
        F.first("sqft", ignorenulls=True).over(w7).alias("first_known_sqft"),
    )
)

metrics_inventory_2 = (
    metrics_inventory
    .select(
        "*",
        F.avg("first_known_sqft").over(w8).alias("last7_avg_first_known_sqft"),
        F.max("first_known_sqft").over(w8).alias("last7_max_first_known_sqft"),
        F.min("first_known_sqft").over(w8).alias("last7_min_first_known_sqft"),
    )
)

result = (
    metrics_inventory_2
    .select(
        "id",
        "sqft_avg_6_prior",
        "sqft_avg_2_prior",
        "sqft_bucket_bucket",
        "first_known_sqft",
        "last7_max_first_known_sqft",
    )
    .orderBy(F.col("id").desc_nulls_first())
)
