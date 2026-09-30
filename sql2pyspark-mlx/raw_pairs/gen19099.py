from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("city", "region").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("city").orderBy("cumulative_sqft", "id")
w4 = Window.partitionBy(F.year("opened_date"))

cleaned = (
    df_stores
    .filter(F.col("city").isin(["Bangkok", "Tokyo"]))
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "sqft",
        F.upper(F.trim("store_name")).alias("store_name_normalized"),
        F.coalesce("region", F.lit("OTHER")).alias("region_filled"),
    )
)

eligible = (
    cleaned
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.order_date") < F.col("s.opened_date")), "left_semi")
)

enriched = (
    eligible
    .filter(F.col("region") != "Central")
    .select(
        "*",
        F.first("region", ignorenulls=True).over(w).alias("first_known_region"),
        F.sum("sqft").over(w2).alias("cumulative_sqft"),
    )
)

scored = (
    enriched
    .withColumn("next_sqft", F.lead("sqft").over(w_running))
    .withColumn("prev_sqft", F.lag("sqft").over(w_running))
    .withColumn("prev_cumulative_sqft", F.lag("cumulative_sqft").over(w_running))
    .withColumn("next_cumulative_sqft", F.lead("cumulative_sqft").over(w_running))
    .withColumn("seq_no", F.row_number().over(w_running))
    .withColumn("opened_date_year_min_cumulative_sqft", F.min("cumulative_sqft").over(w4))
)

result = (
    scored
    .groupBy(
        "region",
        F.when(F.col("prev_cumulative_sqft") >= 60727, "LARGE").otherwise("SMALL").alias("prev_cumulative_sqft_tier"),
        F.date_trunc("WEEK", "opened_date").alias("week_start"),
    )
    .agg(
        F.bit_or("sqft").alias("sqft_bit_or"),
        F.count_if(F.col("city") == "Bangkok").alias("bangkok_events"),
        F.avg("next_sqft").alias("avg_next_sqft"),
        F.round(F.avg("seq_no"), 2).alias("avg_seq_no"),
        F.size(F.collect_set("city")).alias("num_cities"),
    )
    .orderBy(F.col("prev_cumulative_sqft_tier").desc_nulls_first())
)
