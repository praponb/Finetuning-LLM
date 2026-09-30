from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "country").orderBy("rating", "id")

filtered = (
    df_suppliers
    .filter(F.col("city").isNotNull() & F.col("supplier_name").isNotNull())
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        F.concat_ws("/", "country", "city").alias("country_city_key"),
    )
)

scored = (
    filtered
    .filter(F.col("city") == "Bangkok")
    .withColumn("next_rating", F.lead("rating").over(w))
    .withColumn("event_seq", F.row_number().over(w))
)

eligible = (
    scored
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.category").isin(["Books", "Fashion"])), "left_anti")
)

matched_keys = (
    eligible
    .select("id", "city")
    .intersectAll(
        df_suppliers
        .filter(F.col("supplier_name").like("%Euro%"))
        .select("id", "city")
    )
)

reconciled = (
    eligible
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy("country")
    .agg(
        F.round(F.avg("rating"), 2).alias("avg_rating"),
        F.percentile_approx("next_rating", [0.25, 0.5, 0.75]).alias("next_rating_quartiles"),
        F.sum(F.when(F.col("country") == "SG", F.col("event_seq"))).alias("sg_total_event_seq"),
        F.sum(F.when(F.col("country") == "JP", F.col("next_rating"))).alias("jp_total_next_rating"),
        F.count("*").alias("num_rows"),
    )
)
