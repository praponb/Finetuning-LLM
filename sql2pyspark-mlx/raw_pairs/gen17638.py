from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region", "city").orderBy(F.col("sqft").desc(), "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("region").orderBy("sqft", "id").rowsBetween(-3, Window.currentRow)
w4 = Window.partitionBy("region").orderBy(F.col("last4_min_quantity").desc())

kept = (
    df_stores
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.store_id").isin([1, 4, 6])), "left_anti")
)

deduped = (
    kept
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

with_last = (
    deduped
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.region",
        "s.sqft",
        "s.latest_rank",
        F.col("c.last_restock"),
        F.col("c.quantity"),
    )
)

scored = (
    with_last
    .select(
        "id",
        "region",
        "sqft",
        "last_restock",
        "quantity",
        F.min("quantity").over(w3).alias("last4_min_quantity"),
        F.count("quantity").over(w3).alias("last4_count_quantity"),
        F.avg("quantity").over(w3).alias("last4_avg_quantity"),
    )
)

with_latest = (
    scored
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.region",
        "s.sqft",
        "s.last4_min_quantity",
        "s.last4_count_quantity",
        F.col("c.quantity").alias("inventory_quantity"),
        F.col("c.last_restock").alias("inventory_last_restock"),
    )
)

deduped_stores = (
    with_latest
    .withColumn("latest_rank", F.rank().over(w4))
    .filter(F.col("latest_rank") == 1)
)

result = (
    deduped_stores
    .select(
        "id",
        "region",
        "last4_min_quantity",
        "inventory_quantity",
        "inventory_last_restock",
        "latest_rank",
    )
)
