from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("store_id").orderBy("revenue_per_unit", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("store_id").orderBy(F.col("sqft").asc_nulls_last(), "id")
w4 = Window.partitionBy("store_id").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("revenue", "id")

cleaned = (
    df_store_sales
    .filter(F.month("sale_date").isin([1, 7, 11]))
    .select(
        "id",
        "store_id",
        "units",
        "revenue",
        F.ceil("revenue").alias("revenue_whole"),
        F.round((F.col("revenue") / F.col("units")), 2).alias("revenue_per_unit"),
    )
)

scored = (
    cleaned
    .filter(F.col("store_id") == 4)
    .select(
        "*",
        F.count("revenue_per_unit").over(w_running).alias("last3_count_revenue_per_unit"),
        F.sum("revenue_per_unit").over(w_running).alias("last3_sum_revenue_per_unit"),
        F.max("revenue_per_unit").over(w_running).alias("last3_max_revenue_per_unit"),
        F.sum("units").over(w2).alias("cumulative_units"),
    )
)

enriched = (
    scored
    .alias("s")
    .join(df_stores.alias("p2"), F.col("s.store_id") == F.col("p2.id"), "left")
    .select("s.*", F.col("p2.sqft"))
)

ranked = (
    enriched
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
)

with_windows = (
    ranked
    .select(
        "id",
        "units",
        "revenue_whole",
        "last3_sum_revenue_per_unit",
        "cumulative_units",
        "sqft",
        F.sum("last3_sum_revenue_per_unit").over(w4).alias("running_last3_sum_revenue_per_unit"),
        F.lag("sqft", 2, 0).over(w5).alias("prev_sqft"),
        F.lag("revenue", 2).over(w5).alias("prev_revenue"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "units",
        "last3_sum_revenue_per_unit",
        "sqft",
        "prev_sqft",
        "prev_revenue",
    )
    .orderBy(F.col("prev_sqft").desc(), F.col("sqft").desc())
)
