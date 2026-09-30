from pyspark.sql import functions as F

with_dims = (
    df_products
    .alias("a")
    .join(df_suppliers.alias("d"), F.col("a.supplier_id") == F.col("d.id"), "left")
    .select("a.*", F.col("d.supplier_name"), F.col("d.city"), F.col("d.country"))
)

enriched = (
    with_dims
    .alias("a")
    .join(df_suppliers.alias("s"), F.col("a.supplier_id") == F.col("s.id"), "inner")
    .select(
        "a.*",
        F.col("s.rating"),
        F.col("s.country").alias("supplier_country"),
        F.col("s.city").alias("supplier_city"),
    )
)

cleaned = (
    enriched
    .filter(F.lower(F.trim("supplier_name")) == "global supply")
    .select(
        "id",
        "category",
        "brand",
        "cost",
        "launch_date",
        "supplier_id",
        "supplier_name",
        "rating",
        "supplier_country",
        "supplier_city",
        F.round((F.col("price") / F.col("rating")), 2).alias("price_per_rating"),
        F.coalesce("country", F.lit("DIRECT")).alias("country_filled"),
    )
)

result = (
    cleaned
    .withColumn("activity_month", F.date_trunc("MONTH", "launch_date"))
    .groupingSets(
        [["supplier_id", "activity_month"], ["supplier_id"], []],
        "supplier_id",
        "activity_month",
    )
    .agg(
        F.sort_array(F.collect_set("supplier_country")).alias("supplier_country_list"),
        F.count(F.when(F.col("brand") == "Stark", 1)).alias("stark_rows"),
        F.avg(F.when(F.col("supplier_city") == "Berlin", F.col("price_per_rating"))).alias("berlin_avg_price_per_rating"),
    )
    .filter(F.col("stark_rows") >= 1)
    .orderBy(F.col("stark_rows").desc_nulls_last())
)
