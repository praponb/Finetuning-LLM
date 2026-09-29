from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy(F.col("sqft_scaled").desc(), "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.units").desc(), F.col("c.id").desc())

cleaned = (
    df_stores
    .filter((F.col("opened_date") >= F.lit("2023-03-27").cast("date")) & (F.col("sqft") >= 12019))
    .select(
        "*",
        F.round((F.col("sqft") * 1.07), 2).alias("sqft_scaled"),
        F.coalesce("region", F.lit("N/A")).alias("region_filled"),
        F.coalesce("region", F.lit("NONE")).alias("region_filled_2"),
    )
)

deduped = (
    cleaned
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

latest_match = (
    deduped
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.opened_date",
        "s.region_filled",
        "s.latest_rank",
        F.col("c.units"),
        F.col("c.revenue"),
    )
)

filtered = (
    latest_match
    .filter(F.col("opened_date") > F.lit("2023-04-18").cast("date"))
    .select(
        "id",
        "opened_date",
        "region_filled",
        "latest_rank",
        "revenue",
        F.coalesce("revenue", F.lit(0)).alias("revenue_filled"),
    )
)

filtered_stores = (
    filtered
    .filter((F.col("opened_date") >= F.lit("2023-03-28").cast("date")) & (F.col("revenue") <= 2000))
    .select("*", F.least("latest_rank", "revenue").alias("least_latest_rank_revenue"))
)

result = (
    filtered_stores
    .select("id", "region_filled", "latest_rank", "revenue", "revenue_filled")
    .orderBy(F.col("revenue").desc_nulls_first(), F.col("revenue_filled").desc())
)
