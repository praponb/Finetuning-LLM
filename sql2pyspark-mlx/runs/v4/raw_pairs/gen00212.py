from pyspark.sql import functions as F

kept_keys = (
    df_store_sales
    .select("store_id", "id")
    .intersectAll(
        df_store_sales
        .filter(F.col("product_id").isin([2, 5, 9, 10]))
        .select("store_id", "id")
    )
)

kept_rows = (
    df_store_sales
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

base = (
    kept_rows
    .filter(F.col("product_id").isin([4, 5, 8]))
    .select("*", F.date_sub("sale_date", 56).alias("sale_date_minus_56d"))
)

kept_keys_2 = (
    base
    .select("store_id", "id")
    .subtract(
        df_store_sales
        .filter(F.col("units").between(18, 22))
        .select("store_id", "id")
    )
)

verified = (
    base
    .alias("s")
    .join(kept_keys_2.alias("k"), (F.col("s.store_id") == F.col("k.store_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

cleaned = (
    verified
    .filter((F.col("sale_date") > F.to_date(F.lit("2023-08-13"))) & (F.col("units") != 33))
    .select(
        "id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        F.last_day("sale_date_minus_56d").alias("sale_date_minus_56d_month_end"),
        F.floor("revenue").alias("revenue_whole"),
    )
)

with_dims = (
    cleaned
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.launch_date"))
)

result = (
    with_dims
    .select(
        "product_id",
        "sale_date",
        "units",
        "revenue",
        "sale_date_minus_56d_month_end",
        "revenue_whole",
        "launch_date",
    )
)
