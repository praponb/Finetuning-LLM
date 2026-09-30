from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.price").desc(), F.col("c.id").desc())

filtered = (
    df_suppliers
    .filter(F.col("city") == "Tokyo")
    .withColumn("rating_band", F.when(F.col("rating") >= 4.4, "High").when(F.col("rating") >= 1.6, "Medium").otherwise("Low"))
    .withColumn("city_clean", F.coalesce("city", F.lit("DIRECT")))
)

with_latest = (
    filtered
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.launch_date"))
)

base = (
    with_latest
    .filter(F.col("city").isin(["Chiang Mai", "Singapore"]))
    .select(
        "*",
        F.round("rating", 2).alias("rating_rounded"),
        F.date_add("launch_date", 19).alias("launch_date_plus_19d"),
        F.coalesce("city", F.lit("UNKNOWN")).alias("city_clean_2"),
    )
)

remaining_keys = (
    base
    .select("country")
    .intersect(
        df_suppliers
        .filter(F.col("country") == "UK")
        .select("country")
    )
)

reconciled = (
    base
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select(
        "supplier_name",
        "city",
        "rating",
        "rating_band",
        "city_clean",
        "launch_date",
        "launch_date_plus_19d",
        "city_clean_2",
    )
    .orderBy("launch_date_plus_19d")
)
