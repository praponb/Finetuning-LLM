from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("rating").desc(), "id")

cleaned = (
    df_suppliers
    .filter(F.col("city").isNotNull() & (F.col("rating") <= 3.0))
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        F.substring("country", 1, 3).alias("country_prefix"),
        F.round("rating", 2).alias("rating_rounded"),
        F.coalesce("city", F.lit("NONE")).alias("resolved_city"),
    )
)

cleaned_suppliers = (
    cleaned
    .filter(F.col("city").isNotNull())
    .select(
        "*",
        F.trim("supplier_name").alias("clean_supplier_name"),
        F.coalesce("city", F.lit("OTHER")).alias("resolved_city_2"),
    )
)

ranked = (
    cleaned_suppliers
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

not_flagged = (
    ranked
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.supplier_id") == 30), "left_anti")
)

result = (
    not_flagged
    .withColumn("rating_tier", F.when(F.col("rating") >= 4.0, "HIGH").otherwise("LOW"))
    .groupingSets([["rating_tier", "city"], ["rating_tier"], []], "rating_tier", "city")
    .agg(
        F.sum(F.when(F.col("country") == "JP", F.col("rating")).otherwise(0)).alias("jp_rating"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.count("*").alias("event_count"),
    )
    .filter(F.col("event_count") >= 1)
    .orderBy(F.col("jp_rating").desc())
)
