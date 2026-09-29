from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "country")
w2 = Window.partitionBy("city").orderBy(F.col("is_bangkok").desc_nulls_last())
w3 = Window.partitionBy("city").orderBy(F.col("is_bangkok").asc_nulls_first())

filtered = (
    df_suppliers
    .filter(F.col("city").isNotNull() & F.col("rating").isNull())
    .withColumn("is_bangkok", F.when(F.col("city") == "Bangkok", 1).otherwise(0))
    .withColumn("city_filled", F.coalesce("city", F.lit("UNKNOWN")))
)

not_flagged = (
    filtered
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.stock_qty") != 192), "left_anti")
)

kept_keys = (
    not_flagged
    .select("city")
    .intersect(
        df_suppliers
        .filter(F.col("city").isin(["Singapore", "Berlin", "Tokyo"]))
        .select("city")
    )
)

verified = (
    not_flagged
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

base = (
    verified
    .filter(F.col("city") != "London")
    .select(
        "*",
        F.concat_ws(",", "supplier_name", "country").alias("supplier_name_country_key"),
        F.coalesce("city", F.lit("UNKNOWN")).alias("resolved_city"),
    )
)

with_windows = (
    base
    .select(
        "*",
        F.stddev_samp("is_bangkok").over(w).alias("city_stddev_samp_is_bangkok"),
        F.dense_rank().over(w2).alias("is_bangkok_dense_rank"),
        F.rank().over(w3).alias("is_bangkok_rank"),
    )
)

result = (
    with_windows
    .groupBy(
        "city",
        F.when(F.col("rating") >= 3.8, "HIGH").otherwise("LOW").alias("rating_tier"),
    )
    .agg(
        F.max(F.when(F.col("country") == "DE", F.col("is_bangkok_dense_rank"))).alias("de_max_is_bangkok_dense_rank"),
        F.count(F.when(F.col("country") == "TH", 1)).alias("th_rows"),
        F.count("*").alias("event_count"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
    )
    .orderBy(F.col("th_rows").asc_nulls_last())
)
