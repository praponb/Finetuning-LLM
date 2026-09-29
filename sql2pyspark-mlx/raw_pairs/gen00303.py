from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "city")
w2 = Window.partitionBy("country")
w3 = Window.partitionBy("city", "country").orderBy("rating", "id").rowsBetween(-3, Window.currentRow)
w4 = Window.partitionBy("country").orderBy(F.col("country_min_rating").desc_nulls_first(), "id")
w5 = Window.partitionBy("country").orderBy(F.col("th_events").desc())

cleaned = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .withColumn("rating_tier", F.when(F.col("rating") >= 3.4, "High").when(F.col("rating") >= 1.8, "Medium").otherwise("Low"))
)

metrics = (
    cleaned
    .select(
        "id",
        "country",
        "rating",
        F.round((F.col("rating") / F.sum("rating").over(w)), 4).alias("rating_share"),
        F.min("rating").over(w2).alias("country_min_rating"),
        F.avg("rating").over(w3).alias("rating_avg_3"),
    )
)

base = (
    metrics
    .filter(F.col("rating") <= 2.4)
    .select(
        "id",
        "country",
        "rating",
        "rating_share",
        "country_min_rating",
        "rating_avg_3",
        F.initcap(F.trim("country")).alias("country_title"),
    )
)

kept = (
    base
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.cost") <= 580), "left_semi")
)

latest = (
    kept
    .withColumn("rank_in_group", F.row_number().over(w4))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "country", "rating_share", "country_min_rating")
)

result = (
    latest
    .groupBy(
        "country",
        F.when(F.col("rating_share") >= 0.88, "LARGE").otherwise("SMALL").alias("rating_share_tier"),
    )
    .agg(
        F.count("*").alias("event_count"),
        F.count_if(F.col("country") == "TH").alias("th_events"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
    )
    .filter(F.col("event_count") > 1)
    .withColumn("th_events_percent_rank", F.percent_rank().over(w5))
)
