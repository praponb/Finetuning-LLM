from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.units").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("region")

valid = (
    df_stores
    .filter(F.col("opened_date").between("2024-06-01", "2024-12-01"))
    .withColumn("sqft_whole", F.floor("sqft"))
    .withColumn("region_clean", F.coalesce("region", F.lit("UNKNOWN")))
)

with_latest = (
    valid
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.store_name",
        "s.region",
        "s.opened_date",
        "s.sqft",
        F.col("c.sale_date").alias("store_sale_sale_date"),
    )
)

eligible = (
    with_latest
    .alias("s")
    .join(df_store_sales.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.sale_date") < F.col("s.opened_date")), "left_anti")
)

remaining_keys = (
    eligible
    .select("id", "region")
    .subtract(
        df_stores
        .filter(F.col("region").isin(["South", "North", "Central"]))
        .select("id", "region")
    )
)

kept_rows = (
    eligible
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.region") == F.col("k.region")), "inner")
    .select("s.*")
)

metrics = (
    kept_rows
    .filter(F.col("store_sale_sale_date").between("2023-07-15", "2025-02-06"))
    .withColumn("sqft_share", F.round((F.col("sqft") / F.sum("sqft").over(w2)), 4))
    .withColumn("region_min_sqft", F.min("sqft").over(w2))
)

cleaned = (
    metrics
    .filter(F.col("region").isin(["Central", "South", "North"]))
    .select(
        "*",
        (F.col("sqft") / F.nullif("sqft_share", F.lit(0))).alias("sqft_per_sqft_share"),
        F.coalesce("region", F.lit("OTHER")).alias("resolved_region"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "store_name",
        "sqft",
        "sqft_share",
        "region_min_sqft",
        "sqft_per_sqft_share",
        "resolved_region",
        F.when(F.col("store_name").rlike("^(Mall Central|Siam Flagship)$"), "OTHER_ZONE").when(F.col("store_name").rlike("^(Downtown Express|Airport Kiosk)$"), "GROUP_A").otherwise("CORE").alias("store_name_zone"),
    )
    .orderBy(F.col("store_name_zone").asc_nulls_last())
)
