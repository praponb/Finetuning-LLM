from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("city").orderBy(F.col("rating").asc_nulls_last())
w2 = Window.partitionBy("city").orderBy(F.col("is_berlin").asc_nulls_last())

kept = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.product_name").like("%Mat%")), "left_semi")
)

remaining_keys = (
    kept
    .select("country", "city")
    .subtract(
        df_suppliers
        .filter(F.col("city") == "London")
        .select("country", "city")
    )
)

kept_rows = (
    kept
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.country") == F.col("k.country")) & (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

not_flagged = (
    kept_rows
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.stock_qty") < 184), "left_semi")
)

valid = (
    not_flagged
    .filter(F.col("city").isNotNull() & F.col("rating").isNotNull())
    .select(
        "id",
        "supplier_name",
        "city",
        "rating",
        F.when(F.col("city") == "Berlin", 1).otherwise(0).alias("is_berlin"),
        F.regexp_replace("supplier_name", " ", "_").alias("supplier_name_slug"),
    )
)

enriched = (
    valid
    .withColumn("rating_cume_dist", F.cume_dist().over(w_ordered))
    .withColumn("is_berlin_rank", F.rank().over(w2))
)

not_flagged_suppliers = (
    enriched
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.brand") != "Stark"), "left_anti")
)

summary = (
    not_flagged_suppliers
    .groupBy(
        "city",
        F.when(F.col("rating") >= 4.0, "HIGH").otherwise("LOW").alias("rating_tier"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.size(F.collect_set("city")).alias("num_cities"),
    )
)

result = (
    summary
    .select("city", "rating_tier", "cnt", "num_cities")
    .orderBy(F.col("cnt").asc_nulls_first())
)
