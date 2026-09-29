from pyspark.sql import functions as F

cleaned = (
    df_stores
    .filter((F.col("opened_date") >= F.lit("2023-03-26").cast("date")) & F.col("region").isNotNull() & F.col("region").isin(["South", "East"]))
    .select(
        "id",
        "city",
        "region",
        "opened_date",
        "sqft",
        F.trim("store_name").alias("clean_store_name"),
    )
)

result = (
    cleaned
    .groupBy("region")
    .agg(
        F.sum(F.when(F.col("city") == "Berlin", F.col("sqft")).otherwise(0)).alias("berlin_sqft"),
        F.sort_array(F.collect_set("region")).alias("region_list"),
        F.percentile_approx("sqft", [0.25, 0.5, 0.75]).alias("sqft_quartiles"),
    )
    .orderBy(F.col("region").desc_nulls_last())
)
