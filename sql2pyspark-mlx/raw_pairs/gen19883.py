from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.stock_qty").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.launch_date").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("city").orderBy("product_price_2", "id").rowsBetween(-6, Window.currentRow)

with_last = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.price").alias("product_price"))
)

latest_match = (
    with_last
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.brand").alias("product_brand"),
        F.col("c.price").alias("product_price_2"),
    )
)

metrics = (
    latest_match
    .select(
        "id",
        "country",
        "rating",
        "product_brand",
        "product_price_2",
        F.avg("rating").over(w3).alias("last7_avg_rating"),
        F.sum("rating").over(w3).alias("last7_sum_rating"),
        F.max("rating").over(w3).alias("last7_max_rating"),
        F.min("rating").over(w3).alias("last7_min_rating"),
    )
)

kept_keys = (
    metrics
    .select("id", "country")
    .subtract(
        df_suppliers
        .filter(F.col("country").isin(["UK", "SG", "JP"]))
        .select("id", "country")
    )
)

kept_rows = (
    metrics
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

kept_keys_2 = (
    kept_rows
    .select("country", "id")
    .exceptAll(
        df_suppliers
        .filter(F.col("rating") <= 2.8)
        .select("country", "id")
    )
)

verified = (
    kept_rows
    .alias("s")
    .join(kept_keys_2.alias("k"), (F.col("s.country") == F.col("k.country")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy("country", "product_brand")
    .agg(
        F.percentile_approx("last7_avg_rating", 0.9).alias("last7_avg_rating_p90"),
        F.count_if(F.col("country") == "UK").alias("uk_events"),
        F.percentile_approx("last7_avg_rating", [0.25, 0.5, 0.75]).alias("last7_avg_rating_quartiles"),
        F.countDistinct("country").alias("distinct_countries"),
        F.size(F.collect_set("country")).alias("num_countries"),
        F.sum("last7_max_rating").alias("total_last7_max_rating"),
    )
    .filter(F.col("uk_events") < 1000)
    .orderBy(F.col("uk_events").desc())
)
