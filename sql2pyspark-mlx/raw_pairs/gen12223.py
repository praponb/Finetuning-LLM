from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("city").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("city").orderBy("rating")

cleaned = (
    df_suppliers
    .filter(F.col("city").isNotNull() & (F.col("city") != "Bangkok"))
    .select(
        "id",
        "supplier_name",
        "city",
        "rating",
        F.when(F.col("city").isin(["Singapore", "Tokyo"]), "Group A").otherwise("Other").alias("city_group"),
    )
)

enriched = (
    cleaned
    .select(
        "*",
        F.avg("rating").over(w_ordered).alias("running_avg_rating"),
        F.max("rating").over(w_ordered).alias("running_max_rating"),
        F.sum("rating").over(w_ordered).alias("running_sum_rating"),
        F.cume_dist().over(w_recent).alias("rating_cume_dist"),
    )
)

remaining_keys = (
    enriched
    .select("id", "city")
    .exceptAll(
        df_suppliers
        .filter(F.col("rating") >= 4.0)
        .select("id", "city")
    )
)

kept_rows = (
    enriched
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

not_flagged = (
    kept_rows
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.launch_date") <= "2025-08-08"), "left_anti")
)

not_flagged_suppliers = (
    not_flagged
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.stock_qty") != 323), "left_anti")
)

stats = (
    not_flagged_suppliers
    .groupBy(
        F.when(F.col("running_sum_rating") >= 20.2, "PREMIUM").otherwise("STANDARD").alias("running_sum_rating_tier"),
        F.when(F.col("running_max_rating") >= 4.3, "HIGH").otherwise("LOW").alias("running_max_rating_tier"),
        "city",
    )
    .agg(
        F.round(F.avg("running_sum_rating"), 2).alias("avg_running_sum_rating"),
        F.size(F.collect_set("city")).alias("num_cities"),
        F.percentile_approx("rating_cume_dist", 0.5).alias("rating_cume_dist_p50"),
        F.countDistinct("city").alias("distinct_cities"),
        F.sum(F.when(F.col("city") == "London", F.col("rating_cume_dist"))).alias("london_total_rating_cume_dist"),
    )
)

result = (
    stats
    .select(
        "running_sum_rating_tier",
        "running_max_rating_tier",
        "city",
        "avg_running_sum_rating",
        "num_cities",
        "rating_cume_dist_p50",
        "distinct_cities",
        "london_total_rating_cume_dist",
    )
)
