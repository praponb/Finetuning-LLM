from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("city", "region").orderBy("sqft", "id").rowsBetween(-5, Window.currentRow)
w3 = Window.partitionBy("region").orderBy(F.col("sqft").desc(), "id")
w4 = Window.partitionBy("region").orderBy(F.col("weeks_since_opened_date").desc_nulls_first(), "id")
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())

valid = (
    df_stores
    .filter(F.col("sqft").between(5328, 15701))
    .select(
        "*",
        (F.datediff(F.current_date(), "opened_date") / 7).alias("weeks_since_opened_date"),
        F.coalesce("region", F.lit("N/A")).alias("region_filled"),
    )
)

enriched = (
    valid
    .filter(F.col("store_name").isNotNull())
    .select(
        "id",
        "region",
        "opened_date",
        "sqft",
        "weeks_since_opened_date",
        "region_filled",
        F.first("sqft", ignorenulls=True).over(w).alias("first_known_sqft"),
        F.avg("sqft").over(w2).alias("sqft_avg_5"),
        F.ntile(3).over(w3).alias("sqft_bucket"),
    )
)

ranked = (
    enriched
    .withColumn("dedup_rank", F.row_number().over(w4))
    .filter(F.col("dedup_rank") == 1)
)

with_latest = (
    ranked
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.opened_date",
        "s.sqft",
        "s.weeks_since_opened_date",
        "s.region_filled",
        "s.sqft_bucket",
        F.col("c.last_restock").alias("inventory_last_restock"),
    )
)

result = (
    with_latest
    .select(
        "id",
        "opened_date",
        "sqft",
        "weeks_since_opened_date",
        "region_filled",
        "inventory_last_restock",
    )
    .orderBy("weeks_since_opened_date", "sqft")
)
