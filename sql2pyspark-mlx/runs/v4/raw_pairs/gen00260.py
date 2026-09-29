from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("rating", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.orderBy(F.col("rating").desc(), "id")
w3 = Window.partitionBy("country")
w4 = Window.partitionBy("country", "city").orderBy(F.col("rating").desc(), "id")
w5 = Window.partitionBy("city").orderBy(F.col("rating").asc_nulls_first())
w6 = Window.orderBy(F.col("rating").desc())
w7 = Window.partitionBy("city").orderBy(F.col("rating").desc_nulls_last())
w8 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.launch_date").desc(), F.col("c.id").desc())

metrics = (
    df_suppliers
    .select(
        "*",
        F.avg("rating").over(w).alias("rating_avg_2"),
        F.row_number().over(w2).alias("event_seq"),
        F.round((F.col("rating") / F.sum("rating").over(w3)), 4).alias("rating_share"),
        F.ntile(10).over(w4).alias("rating_bucket"),
        F.rank().over(w5).alias("rating_rank"),
    )
)

metrics_suppliers = (
    metrics
    .select(
        "*",
        F.dense_rank().over(w6).alias("rating_dense_rank"),
        F.cume_dist().over(w7).alias("rating_cume_dist"),
    )
)

latest_match = (
    metrics_suppliers
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w8))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.cost"), F.col("c.launch_date"))
)

remaining_keys = (
    latest_match
    .select("id", "city")
    .exceptAll(
        df_suppliers
        .filter(F.col("rating") < 2.5)
        .select("id", "city")
    )
)

kept_rows = (
    latest_match
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy(
        "city",
        F.when(F.col("rating_rank") >= 8, "LARGE").otherwise("SMALL").alias("rating_rank_tier"),
    )
    .agg(
        F.count(F.when(F.col("country") == "JP", 1)).alias("jp_rows"),
        F.size(F.collect_set("country")).alias("num_countries"),
        F.count(F.when(F.col("city") == "New York", 1)).alias("new_york_count"),
        F.round(F.sum("rating_bucket") / F.nullif(F.count(F.when(F.col("country") == "SG", 1)), F.lit(0)), 2).alias("rating_bucket_per_sg"),
        F.percentile_approx("event_seq", 0.5).alias("event_seq_p50"),
        F.count(F.when(F.col("city") == "Bangkok", 1)).alias("bangkok_rows"),
    )
    .orderBy("bangkok_rows")
)
