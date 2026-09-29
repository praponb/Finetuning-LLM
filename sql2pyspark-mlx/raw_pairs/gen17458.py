from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("city").orderBy("sqft", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.partitionBy("city").orderBy("cumulative_sqft", "id")
w4 = Window.orderBy("cumulative_sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_stores
    .filter((F.col("opened_date") > F.lit("2023-03-29").cast("date")) & F.col("region").isNotNull())
    .withColumn("city_length", F.length("city"))
    .withColumn("resolved_region", F.coalesce("region", F.lit("NONE")))
)

not_flagged = (
    cleaned
    .alias("s")
    .join(df_inventory.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.last_restock") < F.col("s.opened_date")), "left_anti")
)

enriched = (
    not_flagged
    .select(
        "id",
        "city",
        "region",
        "opened_date",
        "sqft",
        "resolved_region",
        F.sum("sqft").over(w).alias("cumulative_sqft"),
        F.avg("city_length").over(w2).alias("city_length_avg_3"),
    )
)

filtered = (
    enriched
    .filter(F.col("region").isNotNull() & (F.col("region") == "Central"))
    .select(
        "id",
        "city",
        "region",
        "opened_date",
        "resolved_region",
        "cumulative_sqft",
        F.concat("region", F.lit(" "), "city").alias("region_city"),
        F.coalesce("region", F.lit("NONE")).alias("region_clean"),
    )
)

metrics = (
    filtered
    .select(
        "id",
        "city",
        "resolved_region",
        F.lag("cumulative_sqft").over(w3).alias("prev_cumulative_sqft"),
        F.row_number().over(w3).alias("seq_no"),
        F.first("region", ignorenulls=True).over(w4).alias("first_known_region"),
    )
)

result = (
    metrics
    .select(
        "id",
        "city",
        "resolved_region",
        "prev_cumulative_sqft",
        "first_known_region",
    )
    .orderBy(F.col("city").asc_nulls_last(), F.col("id").desc())
    .limit(20)
)
