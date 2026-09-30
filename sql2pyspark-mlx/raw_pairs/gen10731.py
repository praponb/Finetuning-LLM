from pyspark.sql import functions as F

base = (
    df_stores
    .filter(F.upper("store_name").like("%IRP%"))
    .select(
        "*",
        F.coalesce("sqft", F.lit(0)).alias("sqft_filled"),
        F.date_trunc("YEAR", "opened_date").alias("opened_date_period"),
        F.coalesce("region", F.lit("DIRECT")).alias("resolved_region"),
    )
)

kept_keys = (
    base
    .select("region", "city")
    .intersectAll(
        df_stores
        .filter(F.col("sqft").between(12954, 15722))
        .select("region", "city")
    )
)

kept_rows = (
    base
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.region") == F.col("k.region")) & (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

filtered = (
    kept_rows
    .filter((F.col("opened_date") > F.lit("2023-07-01").cast("date")) & (~F.col("region").eqNullSafe(F.lit("North"))))
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "sqft",
        "sqft_filled",
        "opened_date_period",
        "resolved_region",
        (F.col("sqft") * F.col("sqft_filled")).alias("sqft_times_sqft_filled"),
        F.to_date("opened_date_period").alias("opened_date_period_date"),
    )
)

kept = (
    filtered
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.store_id").isin([2, 3, 4])), "left_anti")
)

not_flagged = (
    kept
    .alias("s")
    .join(df_store_sales.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.sale_date").between("2023-04-25", "2023-07-01")), "left_anti")
)

not_flagged_stores = (
    not_flagged
    .alias("s")
    .join(df_store_sales.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.sale_date") <= F.col("s.opened_date_period")), "left_semi")
)

result = (
    not_flagged_stores
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "sqft",
        "sqft_filled",
        "resolved_region",
        "sqft_times_sqft_filled",
        F.when(F.col("region").rlike("^(North|East)$"), "OTHER_ZONE").when(F.col("region").rlike("^(South|West)$"), "GROUP_A").otherwise("GROUP_B").alias("region_zone"),
    )
    .orderBy("city")
)
