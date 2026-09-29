from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())

filtered = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .select(
        "*",
        F.floor("rating").alias("rating_whole"),
        F.coalesce("city", F.lit("UNKNOWN")).alias("resolved_city"),
    )
)

valid = (
    filtered
    .filter(F.col("city").isNotNull() & (F.col("country") != "SG"))
    .select(
        "id",
        "supplier_name",
        "rating",
        "rating_whole",
        "resolved_city",
        F.initcap(F.trim("country")).alias("country_title"),
        F.coalesce("city", F.lit("DIRECT")).alias("city_filled"),
    )
)

not_flagged = (
    valid
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.cost").between(630, 690)), "left_anti")
)

eligible = (
    not_flagged
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.brand").isin(["Acme", "Umbrella", "Globex"])), "left_anti")
)

valid_suppliers = (
    eligible
    .filter(F.col("supplier_name").isNotNull())
    .select(
        "id",
        "supplier_name",
        "rating_whole",
        "resolved_city",
        "country_title",
        "city_filled",
        F.round((F.col("rating_whole") / F.col("rating")), 2).alias("rating_whole_per_rating"),
    )
)

with_last = (
    valid_suppliers
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.cost"))
)

result = (
    with_last
    .groupBy(
        F.when(F.col("rating_whole_per_rating") >= 80, "LARGE").otherwise("SMALL").alias("rating_whole_per_rating_tier"),
    )
    .agg(
        F.count("*").alias("event_count"),
        F.round(F.avg("rating_whole"), 2).alias("avg_rating_whole"),
    )
)
