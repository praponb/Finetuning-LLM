from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "city").orderBy(F.col("rating").desc_nulls_first(), "id")

deduped = (
    df_suppliers
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

not_flagged = (
    deduped
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.brand").isNull()), "left_anti")
)

remaining_keys = (
    not_flagged
    .select("city", "id")
    .intersect(
        df_suppliers
        .filter(F.col("country") == "UK")
        .select("city", "id")
    )
)

kept_rows = (
    not_flagged
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.city") == F.col("k.city")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

matched_keys = (
    kept_rows
    .select("id", "country")
    .intersect(
        df_suppliers
        .filter(F.col("city") == "Singapore")
        .select("id", "country")
    )
)

kept_rows_suppliers = (
    kept_rows
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

filtered = (
    kept_rows_suppliers
    .filter(F.col("rating") <= 3.1)
    .select(
        "id",
        "supplier_name",
        "city",
        "rating",
        F.when(F.col("rating") >= 4.4, "High").when(F.col("rating") >= 1.8, "Medium").otherwise("Low").alias("rating_tier"),
        (F.col("rating") - F.col("rank_in_group")).alias("rating_minus_rank_in_group"),
        F.coalesce("city", F.lit("UNKNOWN")).alias("city_clean"),
    )
)

result = (
    filtered
    .groupBy("city")
    .agg(
        F.avg("rating").alias("avg_rating"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("rating_minus_rank_in_group", [0.25, 0.5, 0.75]).alias("rating_minus_rank_in_group_quartiles"),
        F.size(F.collect_set("city")).alias("num_cities"),
        F.sum(F.when(F.col("city") == "Chiang Mai", F.col("rating")).otherwise(0)).alias("chiang_mai_rating"),
        F.countDistinct("id").alias("distinct_ids_2"),
    )
    .orderBy(F.col("distinct_ids_2").desc_nulls_last())
)
