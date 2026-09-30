from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.year("opened_date"), "region").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("city")
w3 = Window.orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("city").orderBy(F.col("first_known_sqft").asc_nulls_last())
w5 = Window.partitionBy("city").orderBy(F.col("first_known_sqft").desc_nulls_first(), "id")

scored = (
    df_stores
    .select(
        "id",
        "store_name",
        "city",
        "opened_date",
        F.first("sqft", ignorenulls=True).over(w).alias("first_known_sqft"),
        F.round((F.col("sqft") / F.sum("sqft").over(w2)), 4).alias("sqft_share"),
        F.first("region", ignorenulls=True).over(w3).alias("first_known_region"),
    )
)

latest = (
    scored
    .withColumn("dedup_rank", F.rank().over(w4))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "city", "first_known_sqft", "first_known_region")
)

ranked = (
    latest
    .withColumn("latest_rank", F.row_number().over(w5))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

not_flagged = (
    ranked
    .alias("s")
    .join(df_store_sales.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.units") > 34), "left_anti")
)

grouped = (
    not_flagged
    .groupBy("city")
    .agg(
        F.size(F.collect_set("city")).alias("num_cities"),
        F.percentile_approx("first_known_sqft", [0.25, 0.5, 0.75]).alias("first_known_sqft_quartiles"),
        F.size(F.collect_set("city")).alias("num_cities_2"),
        F.sum(F.when(F.col("city") == "Tokyo", F.col("first_known_sqft")).otherwise(0)).alias("tokyo_first_known_sqft"),
    )
    .filter(F.col("num_cities_2") < 1000)
)

result = (
    grouped
    .select(
        "city",
        "num_cities",
        "first_known_sqft_quartiles",
        "num_cities_2",
        "tokyo_first_known_sqft",
        F.when(F.col("city").rlike("^(Chiang Mai|Singapore)$"), "GROUP_B").otherwise("EXTENDED").alias("city_zone"),
    )
)
