from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country")
w2 = Window.partitionBy("city").orderBy("is_de", "id")
w3 = Window.partitionBy("country").orderBy("country_min_rating", "id")

cleaned = (
    df_suppliers
    .filter(~F.col("city").eqNullSafe(F.lit("Chiang Mai")))
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        F.when(F.col("rating") >= 3.8, "High").when(F.col("rating") >= 2.4, "Medium").otherwise("Low").alias("rating_band"),
    )
)

valid = (
    cleaned
    .filter(~F.col("city").eqNullSafe(F.lit("Chiang Mai")))
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        F.when(F.col("country") == "DE", 1).otherwise(0).alias("is_de"),
        F.trim("country").alias("clean_country"),
    )
)

base = (
    valid
    .filter(F.col("supplier_name").like("%Gmb%"))
    .select(
        "*",
        F.when(F.col("city").isin(["Tokyo", "Berlin"]), "Core").otherwise("Other").alias("city_group"),
        F.upper("country").alias("country_upper"),
        F.coalesce("city", F.lit("NONE")).alias("city_clean"),
    )
)

scored = (
    base
    .select(
        "id",
        "supplier_name",
        "country",
        "rating",
        "is_de",
        "city_group",
        F.min("rating").over(w).alias("country_min_rating"),
        F.lag("rating", 1).over(w2).alias("prev_rating"),
    )
)

eligible = (
    scored
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.launch_date") >= "2024-01-01"), "left_semi")
)

metrics = (
    eligible
    .select(
        "id",
        "supplier_name",
        "country",
        "rating",
        "is_de",
        F.lead("rating").over(w3).alias("next_rating"),
        F.lag("country_min_rating").over(w3).alias("prev_country_min_rating"),
        F.row_number().over(w3).alias("event_seq"),
    )
)

result = (
    metrics
    .select(
        "id",
        "country",
        "rating",
        F.when(F.col("country").rlike("^(DE|TH)$"), "EXTENDED").otherwise("GROUP_B").alias("country_zone"),
    )
    .orderBy(F.col("id").desc(), F.col("rating").desc())
)
