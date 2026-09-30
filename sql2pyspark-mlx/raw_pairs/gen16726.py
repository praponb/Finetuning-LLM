from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")
w2 = Window.partitionBy("region")

base = (
    df_stores
    .filter(F.col("region").isNotNull() & (F.col("region").eqNullSafe(F.lit("East"))))
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "sqft",
        F.round("sqft", 1).alias("sqft_rounded"),
    )
)

remaining_keys = (
    base
    .select("city", "id")
    .intersectAll(
        df_stores
        .filter(F.col("city").isin(["Tokyo", "New York"]))
        .select("city", "id")
    )
)

reconciled = (
    base
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.city") == F.col("k.city")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

filtered = (
    reconciled
    .filter(F.col("region").eqNullSafe(F.lit("West")))
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "sqft",
        F.initcap(F.trim("store_name")).alias("store_name_title"),
    )
)

metrics = (
    filtered
    .withColumn("sqft_share", F.round((F.col("sqft") / F.sum("sqft").over(w)), 4))
    .withColumn("sqft_share_2", F.round((F.col("sqft") / F.sum("sqft").over(w2)), 4))
)

result = (
    metrics
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "sqft",
        "store_name_title",
        "sqft_share",
        "sqft_share_2",
    )
    .orderBy(F.col("region").asc_nulls_last())
)
