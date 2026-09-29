from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("sale_date").desc())
w2 = Window.partitionBy("store_id").orderBy("units_rounded", "id")

filtered = (
    df_store_sales
    .filter(F.col("sale_date") >= F.lit("2023-04-02").cast("date"))
    .select(
        "id",
        "store_id",
        "sale_date",
        "revenue",
        F.round("units", 0).alias("units_rounded"),
        F.date_trunc("week", "sale_date").alias("sale_date_period"),
    )
)

latest = (
    filtered
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
)

joined = (
    latest
    .alias("a")
    .join(df_stores.alias("s"), F.col("a.store_id") == F.col("s.id"), "left")
    .select("a.*", F.col("s.opened_date"), F.col("s.city"))
)

with_windows = (
    joined
    .filter(F.col("revenue") > 1800)
    .select(
        "*",
        F.lead("revenue").over(w2).alias("next_revenue"),
        F.lead("units_rounded").over(w2).alias("next_units_rounded"),
        F.row_number().over(w2).alias("event_seq"),
    )
)

result = (
    with_windows
    .groupBy("store_id")
    .agg(
        F.min("sale_date_period").alias("first_sale_date_period"),
        F.max(F.when(F.col("city") == "Berlin", F.col("next_units_rounded"))).alias("berlin_max_next_units_rounded"),
        F.sum(F.when(F.col("city") == "Singapore", F.col("next_units_rounded"))).alias("singapore_total_next_units_rounded"),
        F.count(F.when(F.col("city") == "Chiang Mai", 1)).alias("chiang_mai_count"),
        F.count(F.when(F.col("city") == "New York", 1)).alias("new_york_count"),
    )
)
