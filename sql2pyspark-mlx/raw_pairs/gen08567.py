from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.units").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("region", "city").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("city").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

eligible = (
    df_stores
    .alias("s")
    .join(df_store_sales.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.sale_date") >= F.col("s.opened_date")), "left_anti")
)

with_last = (
    eligible
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.units").alias("store_sale_units"))
)

kept = (
    with_last
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.units") == 12), "left_anti")
)

valid = (
    kept
    .filter(F.col("opened_date") > F.lit("2023-02-19").cast("date"))
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "sqft",
        F.initcap(F.trim("store_name")).alias("store_name_title"),
        F.last_day("opened_date").alias("opened_date_month_end"),
    )
)

kept_stores = (
    valid
    .alias("s")
    .join(df_inventory.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.last_restock") < F.col("s.opened_date_month_end")), "left_semi")
)

enriched = (
    kept_stores
    .select(
        "id",
        "store_name",
        "city",
        "sqft",
        "store_name_title",
        F.max("sqft").over(w2).alias("running_max_sqft"),
        F.sum("sqft").over(w2).alias("running_sum_sqft"),
        F.min("sqft").over(w2).alias("running_min_sqft"),
        F.first("sqft", ignorenulls=True).over(w3).alias("first_known_sqft"),
    )
)

result = (
    enriched
    .groupBy("city")
    .agg(
        F.count(F.when(F.col("city") == "Chiang Mai", 1)).alias("chiang_mai_count"),
        F.count_if(F.col("city") == "Bangkok").alias("bangkok_events"),
        F.percentile_approx("running_min_sqft", 0.95).alias("running_min_sqft_p95"),
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
        F.round(F.sum("sqft") / F.nullif(F.count(F.when(F.col("city") == "New York", 1)), F.lit(0)), 2).alias("sqft_per_new_york"),
    )
)
