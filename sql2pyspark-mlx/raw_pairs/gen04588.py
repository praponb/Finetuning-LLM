from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.orderBy(F.col("rating").desc_nulls_last())
w2 = Window.partitionBy("country").orderBy(F.col("rating").desc_nulls_first())
w3 = Window.partitionBy("country")

matched_keys = (
    df_suppliers
    .select("id", "city")
    .intersectAll(
        df_suppliers
        .filter(F.col("supplier_name").like("Glo%"))
        .select("id", "city")
    )
)

kept_rows = (
    df_suppliers
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

cleaned = (
    kept_rows
    .filter(F.col("city").isNotNull() & ~F.col("supplier_name").like("%Sup%"))
    .withColumn("country_upper", F.upper("country"))
    .withColumn("resolved_city", F.coalesce("city", F.lit("UNKNOWN")))
)

metrics = (
    cleaned
    .select(
        "*",
        F.dense_rank().over(w_grp).alias("rating_dense_rank"),
        F.rank().over(w2).alias("rating_rank"),
        F.round((F.col("rating") / F.sum("rating").over(w3)), 4).alias("rating_share"),
    )
)

not_flagged = (
    metrics
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.price") > 560), "left_anti")
)

base = (
    not_flagged
    .filter(F.col("country").isin(["DE", "JP"]))
    .select(
        "*",
        F.least("rating_rank", "rating_share").alias("least_rating_rank_rating_share"),
    )
)

stats = (
    base
    .groupBy(
        F.when(F.col("least_rating_rank_rating_share") >= 78, "LARGE").otherwise("SMALL").alias("least_rating_rank_rating_share_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.round(F.sum("rating_share") / F.nullif(F.count(F.when(F.col("country") == "US", 1)), F.lit(0)), 2).alias("rating_share_per_us"),
        F.size(F.collect_set("country")).alias("num_countries"),
    )
)

result = (
    stats
    .select(
        "least_rating_rank_rating_share_tier",
        "city_list",
        "rating_share_per_us",
        "num_countries",
    )
)
