from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("opened_date", "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())
w3 = Window.partitionBy(F.month("opened_date"), "city").orderBy("opened_date", "id").rowsBetween(-6, Window.currentRow)
w4 = Window.partitionBy("region").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("region").orderBy("inventory_quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy(F.col("sqft").asc_nulls_last())
w7 = Window.partitionBy("city", "region").orderBy("inventory_quantity", "id")

deduped = (
    df_stores
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

with_last = (
    deduped
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.city",
        "s.region",
        "s.opened_date",
        "s.sqft",
        F.col("c.quantity").alias("inventory_quantity"),
    )
)

metrics = (
    with_last
    .filter(F.col("sqft") > 15411)
    .select(
        "id",
        "city",
        "region",
        "opened_date",
        "sqft",
        "inventory_quantity",
        F.avg("inventory_quantity").over(w3).alias("inventory_quantity_avg_6"),
        F.first("sqft", ignorenulls=True).over(w4).alias("first_known_sqft"),
        F.last("region", ignorenulls=True).over(w5).alias("last_known_region"),
        F.dense_rank().over(w6).alias("sqft_dense_rank"),
    )
)

deduped_stores = (
    metrics
    .withColumn("rn", F.row_number().over(w7))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    deduped_stores
    .withColumn("week_start", F.date_trunc("WEEK", "opened_date"))
    .withColumn("inventory_quantity_tier", F.when(F.col("inventory_quantity") >= 236, "HIGH").otherwise("LOW"))
    .rollup("week_start", "inventory_quantity_tier")
    .agg(
        F.count(F.when(F.col("region") == "East", 1)).alias("east_count"),
        F.round(F.sum("sqft_dense_rank") / F.nullif(F.count(F.when(F.col("city") == "Tokyo", 1)), F.lit(0)), 2).alias("sqft_dense_rank_per_tokyo"),
        F.count_if(F.col("region") == "Central").alias("central_events"),
        F.grouping_id().alias("grouping_id"),
    )
    .orderBy(F.col("east_count").asc_nulls_first())
)
