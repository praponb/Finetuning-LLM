from pyspark.sql import functions as F

units_by_store_id = df_store_sales.groupBy("store_id").agg(F.min("units").alias("min_store_sale_units"))

cleaned = (
    df_store_sales
    .filter(F.year("sale_date") == 2025)
    .withColumn("revenue_per_unit", F.round((F.col("revenue") / F.col("units")), 2))
)

with_dims = (
    cleaned
    .alias("a")
    .join(df_stores.alias("b"), F.col("a.store_id") == F.col("b.id"), "left")
    .filter(F.col("b.sqft") >= 7517)
    .select("a.*", F.col("b.region"), F.col("b.opened_date"))
)

matched_keys = (
    with_dims
    .select("store_id", "id")
    .subtract(
        df_store_sales
        .filter(F.col("revenue") < 2700)
        .select("store_id", "id")
    )
)

verified = (
    with_dims
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

matched_keys_2 = (
    verified
    .select("id", "product_id")
    .intersectAll(
        df_store_sales
        .filter(F.col("store_id").isin([2, 5, 6]))
        .select("id", "product_id")
    )
)

reconciled = (
    verified
    .alias("s")
    .join(matched_keys_2.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

base = (
    reconciled
    .filter((F.col("sale_date") >= F.lit("2023-05-23").cast("date")) & F.col("product_id").isin([1, 8, 9]))
    .withColumn("adjusted_units", F.round((F.col("units") * 1.07), 2))
    .withColumn("region_lower", F.lower("region"))
)

result = (
    base
    .join(units_by_store_id, on="store_id", how="left")
    .select(
        "id",
        "store_id",
        "product_id",
        "units",
        "revenue",
        "revenue_per_unit",
        "opened_date",
        "adjusted_units",
        F.col("min_store_sale_units"),
    )
    .orderBy(F.col("min_store_sale_units").desc_nulls_first(), "units")
)
