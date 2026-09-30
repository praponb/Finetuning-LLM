from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("opened_date", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy(F.date_trunc("month", "opened_date"), "city").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

not_flagged = (
    df_stores
    .alias("s")
    .join(df_inventory.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.quantity") != 88), "left_anti")
)

kept = (
    not_flagged
    .alias("s")
    .join(df_store_sales.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.sale_date") >= F.col("s.opened_date")), "left_semi")
)

enriched = (
    kept
    .filter(F.col("region").isin(["Central", "North", "East"]))
    .select(
        "id",
        "city",
        "region",
        "opened_date",
        "sqft",
        F.min("sqft").over(w).alias("last3_min_sqft"),
        F.avg("sqft").over(w).alias("last3_avg_sqft"),
        F.count("sqft").over(w).alias("last3_count_sqft"),
        F.sum("sqft").over(w).alias("last3_sum_sqft"),
        F.last("region", ignorenulls=True).over(w2).alias("last_known_region"),
    )
)

eligible = (
    enriched
    .alias("s")
    .join(df_inventory.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.last_restock") >= "2023-04-01"), "left_anti")
)

result = (
    eligible
    .select(
        "id",
        "city",
        "region",
        "last3_min_sqft",
        "last3_avg_sqft",
        "last3_count_sqft",
        "last3_sum_sqft",
        "last_known_region",
    )
    .orderBy(F.col("last3_count_sqft").asc_nulls_last())
)
