from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("rating", "id")
w2 = Window.orderBy("rating", "id")
w3 = Window.orderBy("city_length", "id")
w4 = Window.partitionBy("country").orderBy("prev_city_length", "id")
w5 = Window.orderBy("next_rating", "id")

base = (
    df_suppliers
    .filter(F.col("rating") > 1.9)
    .withColumn("city_length", F.length("city"))
    .withColumn("resolved_city", F.coalesce("city", F.lit("N/A")))
)

enriched = (
    base
    .select(
        "*",
        F.lead("rating", 1).over(w).alias("next_rating"),
        F.lag("city_length", 1, 0).over(w2).alias("prev_city_length"),
        F.row_number().over(w3).alias("event_seq"),
    )
)

with_windows = (
    enriched
    .withColumn("prev_prev_city_length", F.lag("prev_city_length", 2).over(w4))
    .withColumn("next_rating_2", F.lead("rating", 1).over(w5))
)

summary = (
    with_windows
    .groupBy(
        F.when(F.col("event_seq") >= 7, "PREMIUM").otherwise("STANDARD").alias("event_seq_tier"),
    )
    .agg(
        F.round(F.avg("rating"), 2).alias("avg_rating"),
        F.count(F.when(F.col("country") == "US", 1)).alias("us_rows"),
        F.count(F.when(F.col("country") == "UK", 1)).alias("uk_count"),
    )
)

result = (
    summary
    .select("event_seq_tier", "avg_rating", "us_rows", "uk_count")
    .orderBy(F.col("event_seq_tier").desc_nulls_last(), F.col("us_rows").desc())
    .limit(10)
)
