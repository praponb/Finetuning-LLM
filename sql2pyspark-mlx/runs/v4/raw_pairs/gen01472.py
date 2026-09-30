from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("rating").asc_nulls_first(), "id")
w2 = Window.partitionBy("country").orderBy(F.col("rank_in_group").desc(), "id")
w3 = Window.partitionBy("city").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("country", "city")
w5 = Window.orderBy(F.col("rating").asc_nulls_last())
w6 = Window.partitionBy("city").orderBy("rn", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("city").orderBy(F.col("rating").desc(), "id")
w8 = Window.partitionBy("country").orderBy("rn", "id")
w9 = Window.partitionBy("city").orderBy("rank_in_group", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("country").orderBy("first_known_rating_cume_dist", "id")
w11 = Window.partitionBy("country", "city").orderBy("first_known_rating_cume_dist", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w12 = Window.partitionBy("city", "country").orderBy("rank_in_group", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w13 = Window.partitionBy("country", "city").orderBy(F.col("first_known_rating_cume_dist").asc_nulls_last())
w14 = Window.partitionBy("city").orderBy(F.col("event_seq").desc(), "id")

deduped = (
    df_suppliers
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

deduped_suppliers = (
    deduped
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
)

kept = (
    deduped_suppliers
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.month(F.col("c.launch_date")).isin([1, 5, 8])), "left_semi")
)

with_windows = (
    kept
    .select(
        "*",
        F.last("rating", ignorenulls=True).over(w3).alias("last_known_rating"),
        F.round((F.col("rating") / F.sum("rating").over(w4)), 4).alias("rating_share"),
        F.cume_dist().over(w5).alias("rating_cume_dist"),
        F.first("city", ignorenulls=True).over(w6).alias("first_known_city"),
    )
)

with_windows_suppliers = (
    with_windows
    .select(
        "*",
        F.row_number().over(w7).alias("event_seq"),
        F.lead("rating_share", 1).over(w8).alias("next_rating_share"),
        F.first("rating_cume_dist", ignorenulls=True).over(w9).alias("first_known_rating_cume_dist"),
    )
)

scored = (
    with_windows_suppliers
    .withColumn("event_seq_2", F.row_number().over(w10))
    .withColumn("cumulative_rating_share", F.sum("rating_share").over(w11))
)

enriched = (
    scored
    .select(
        "*",
        F.last("rating_cume_dist", ignorenulls=True).over(w12).alias("last_known_rating_cume_dist"),
        F.dense_rank().over(w13).alias("first_known_rating_cume_dist_dense_rank"),
        F.row_number().over(w14).alias("row_seq"),
    )
)

daily = (
    enriched
    .groupBy(
        "country",
        F.when(F.col("rating_share") >= 0.69, "PREMIUM").otherwise("STANDARD").alias("rating_share_tier"),
        F.when(F.col("rating_cume_dist") >= 0.8, "HIGH").otherwise("LOW").alias("rating_cume_dist_tier"),
    )
    .agg(
        F.min("cumulative_rating_share").alias("min_cumulative_rating_share"),
        F.count(F.when(F.col("city") == "Berlin", 1)).alias("berlin_rows"),
        F.count("*").alias("cnt"),
        F.avg(F.when(F.col("city") == "Tokyo", F.col("first_known_rating_cume_dist"))).alias("tokyo_avg_first_known_rating_cume_dist"),
        F.count("*").alias("num_rows"),
    )
    .filter(F.col("min_cumulative_rating_share") < 0)
)

result = (
    daily
    .select(
        "country",
        "rating_share_tier",
        "rating_cume_dist_tier",
        "min_cumulative_rating_share",
        "cnt",
        "tokyo_avg_first_known_rating_cume_dist",
        "num_rows",
    )
    .orderBy(F.col("cnt").desc_nulls_last())
)
