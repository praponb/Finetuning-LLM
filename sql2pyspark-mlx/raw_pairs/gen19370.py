from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("city", "country").orderBy("rating", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("country")
w3 = Window.partitionBy("country").orderBy(F.col("rating").desc(), "id")
w4 = Window.partitionBy("country").orderBy("country_max_last7_avg_rating", "id")

eligible = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.cost").between(540, 570)), "left_anti")
)

metrics = (
    eligible
    .filter(F.col("country").isin(["DE", "US"]))
    .select(
        "id",
        "country",
        "rating",
        F.avg("rating").over(w_recent).alias("last7_avg_rating"),
        F.count("rating").over(w_recent).alias("last7_count_rating"),
        F.max("rating").over(w_recent).alias("last7_max_rating"),
    )
)

metrics_suppliers = (
    metrics
    .select(
        "id",
        "country",
        "rating",
        "last7_avg_rating",
        "last7_count_rating",
        F.max("last7_avg_rating").over(w2).alias("country_max_last7_avg_rating"),
        F.ntile(5).over(w3).alias("rating_bucket"),
    )
)

deduped = (
    metrics_suppliers
    .withColumn("rank_in_group", F.row_number().over(w4))
    .filter(F.col("rank_in_group") == 1)
)

eligible_suppliers = (
    deduped
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.length(F.col("ch.product_name")) < 14), "left_anti")
)

result = (
    eligible_suppliers
    .rollup(
        "country",
        F.when(F.col("rating_bucket") >= 6, "LARGE").otherwise("SMALL").alias("rating_bucket_tier"),
    )
    .agg(
        F.sum(F.when(F.col("country") == "TH", F.col("last7_avg_rating")).otherwise(0)).alias("th_last7_avg_rating"),
        F.round(F.sum("country_max_last7_avg_rating") / F.nullif(F.count(F.when(F.col("country") == "US", 1)), F.lit(0)), 2).alias("country_max_last7_avg_rating_per_us"),
        F.sum(F.when(F.col("country") == "US", F.col("country_max_last7_avg_rating")).otherwise(0)).alias("us_country_max_last7_avg_rating"),
        F.count_if(F.col("country") == "UK").alias("uk_events"),
        F.percentile_approx("last7_avg_rating", [0.25, 0.5, 0.75]).alias("last7_avg_rating_quartiles"),
        F.min("last7_count_rating").alias("min_last7_count_rating"),
    )
    .filter(F.col("th_last7_avg_rating") < 1000000)
    .orderBy(F.col("country_max_last7_avg_rating_per_us").desc_nulls_first())
)
