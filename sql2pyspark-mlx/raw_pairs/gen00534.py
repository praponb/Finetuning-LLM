from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("country").orderBy(F.col("running_sum_rating").desc())
w3 = Window.partitionBy("country").orderBy(F.col("running_count_rating").desc())
w4 = Window.orderBy(F.col("running_count_rating").asc_nulls_last())
w5 = Window.partitionBy("country")

filtered = (
    df_suppliers
    .filter(F.col("city") != "London")
    .select(
        "id",
        "supplier_name",
        "country",
        "rating",
        F.concat_ws("/", "supplier_name", "city").alias("supplier_name_city_key"),
        F.concat_ws("-", "country", "city").alias("country_city_key"),
        F.coalesce("city", F.lit("NONE")).alias("resolved_city"),
    )
)

enriched = (
    filtered
    .withColumn("running_sum_rating", F.sum("rating").over(w))
    .withColumn("running_max_rating", F.max("rating").over(w))
    .withColumn("running_count_rating", F.count("rating").over(w))
    .withColumn("running_min_rating", F.min("rating").over(w))
)

kept_keys = (
    enriched
    .select("country")
    .subtract(
        df_suppliers
        .filter(F.col("rating") < 3.5)
        .select("country")
    )
)

reconciled = (
    enriched
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

with_windows = (
    reconciled
    .filter(F.col("country").isin(["US", "DE", "TH"]))
    .select(
        "id",
        "country",
        "rating",
        "supplier_name_city_key",
        "country_city_key",
        "resolved_city",
        "running_count_rating",
        F.rank().over(w2).alias("running_sum_rating_rank"),
        F.dense_rank().over(w3).alias("running_count_rating_dense_rank"),
    )
)

kept = (
    with_windows
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.cost") <= 300), "left_semi")
)

not_flagged = (
    kept
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (~F.col("c.category").isin(["Fashion", "Home", "Sports"])), "left_anti")
)

enriched_suppliers = (
    not_flagged
    .select(
        "*",
        F.rank().over(w4).alias("running_count_rating_rank"),
        F.round((F.col("rating") / F.sum("rating").over(w5)), 4).alias("rating_share"),
    )
)

result = (
    enriched_suppliers
    .select(
        "id",
        "country",
        "rating",
        "country_city_key",
        "resolved_city",
        "running_sum_rating_rank",
        "running_count_rating_rank",
        "rating_share",
        F.when(F.col("country").rlike("^(UK|JP)$"), "CORE").otherwise("PRIORITY").alias("country_zone"),
    )
    .orderBy(F.col("id").desc_nulls_last())
)
