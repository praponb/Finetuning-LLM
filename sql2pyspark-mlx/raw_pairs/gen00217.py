from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region")

base = (
    df_stores
    .filter(F.col("store_name").isNotNull())
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "sqft",
        F.initcap(F.trim("store_name")).alias("store_name_title"),
        F.coalesce("region", F.lit("DIRECT")).alias("resolved_region"),
    )
)

remaining_keys = (
    base
    .select("city", "region")
    .subtract(
        df_stores
        .filter(F.month("opened_date").isin([4, 9, 12]))
        .select("city", "region")
    )
)

verified = (
    base
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.city") == F.col("k.city")) & (F.col("s.region") == F.col("k.region")), "inner")
    .select("s.*")
)

valid = (
    verified
    .filter((F.col("opened_date") > F.to_date(F.lit("2023-04-30"))) & (F.col("city") != "Berlin"))
    .select(
        "id",
        "store_name",
        "region",
        "opened_date",
        "sqft",
        "store_name_title",
        "resolved_region",
        F.when(F.col("region").isin(["North", "East"]), "Tier 1").otherwise("Other").alias("region_group"),
    )
)

metrics = (
    valid
    .select(
        "*",
        F.round((F.col("sqft") / F.sum("sqft").over(w)), 4).alias("sqft_share"),
        F.round((F.col("sqft") / F.sum("sqft").over(w)), 4).alias("sqft_share_2"),
    )
)

result = (
    metrics
    .select("region", "sqft_share", "sqft_share_2")
    .unpivot(
        ids=["region"],
        values=["sqft_share", "sqft_share_2"],
        variableColumnName="kpi",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)
