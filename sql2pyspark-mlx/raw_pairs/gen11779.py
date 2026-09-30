from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("rating").asc_nulls_first(), "id")

filtered = (
    df_suppliers
    .filter(F.col("city").isNull())
    .select(
        "id",
        "supplier_name",
        "city",
        "rating",
        F.upper("supplier_name").alias("supplier_name_upper"),
        F.trim("city").alias("clean_city"),
        F.coalesce("city", F.lit("OTHER")).alias("resolved_city"),
    )
)

deduped = (
    filtered
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "supplier_name", "city", "rating", "supplier_name_upper")
)

deduped_suppliers = (
    deduped
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

kept_keys = (
    deduped_suppliers
    .select("id", "city")
    .intersectAll(
        df_suppliers
        .filter(~F.col("city").isin(["Tokyo", "Bangkok", "New York"]))
        .select("id", "city")
    )
)

verified = (
    deduped_suppliers
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

eligible = (
    verified
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.category") == "Books"), "left_semi")
)

result = (
    eligible
    .groupBy(
        F.when(F.col("rating") >= 4.2, "PREMIUM").otherwise("STANDARD").alias("rating_tier"),
    )
    .agg(
        F.sum(F.when(F.col("city") == "Bangkok", F.col("rating")).otherwise(0)).alias("bangkok_rating"),
        F.round(F.avg("rating"), 2).alias("avg_rating"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.sum(F.when(F.col("city") == "Chiang Mai", F.col("rating"))).alias("chiang_mai_total_rating"),
    )
    .orderBy(F.col("bangkok_rating").desc())
)
