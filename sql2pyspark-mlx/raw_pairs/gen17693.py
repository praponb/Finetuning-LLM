from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "city")
w_hist = Window.partitionBy("city", "country").orderBy(F.col("is_singapore").asc_nulls_last())
w3 = Window.partitionBy("city").orderBy(F.col("is_singapore").desc_nulls_last())
w_ordered = Window.orderBy(F.col("rating").desc_nulls_last())
w5 = Window.partitionBy("country")

valid = (
    df_suppliers
    .filter(F.col("city") == "New York")
    .select(
        "*",
        F.when(F.col("country").isin(["SG", "TH"]), "Core").otherwise("Other").alias("country_group"),
        F.when(F.col("city") == "Singapore", 1).otherwise(0).alias("is_singapore"),
    )
)

remaining_keys = (
    valid
    .select("country", "city")
    .subtract(
        df_suppliers
        .filter(F.col("supplier_name").like("%Imports%"))
        .select("country", "city")
    )
)

kept_rows = (
    valid
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.country") == F.col("k.country")) & (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

eligible = (
    kept_rows
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.stock_qty") < 220), "left_anti")
)

kept = (
    eligible
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.launch_date") < "2023-08-15"), "left_anti")
)

with_windows = (
    kept
    .select(
        "id",
        "city",
        "is_singapore",
        F.stddev_samp("rating").over(w).alias("country_stddev_samp_rating"),
        F.rank().over(w_hist).alias("is_singapore_rank"),
        F.dense_rank().over(w3).alias("is_singapore_dense_rank"),
        F.rank().over(w_ordered).alias("rating_rank"),
        F.round((F.col("rating") / F.sum("rating").over(w5)), 4).alias("rating_share"),
    )
)

kept_suppliers = (
    with_windows
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.stock_qty").isNotNull()), "left_semi")
)

base = (
    kept_suppliers
    .filter(F.col("city").isNotNull() & (~F.col("city").eqNullSafe(F.lit("London"))))
    .withColumn("city_length", F.length("city"))
    .withColumn("city_clean", F.coalesce("city", F.lit("DIRECT")))
)

daily = (
    base
    .groupBy("city")
    .agg(
        F.count_if(F.col("city") == "New York").alias("new_york_events"),
        F.sum(F.when(F.col("city") == "Singapore", F.col("city_length")).otherwise(0)).alias("singapore_city_length"),
        F.max("is_singapore_dense_rank").alias("max_is_singapore_dense_rank"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
    )
)

result = (
    daily
    .select("city", "singapore_city_length")
    .orderBy("singapore_city_length", F.col("city").desc())
)
