from pyspark.sql import functions as F

kept_keys = (
    df_store_sales
    .select("id")
    .intersectAll(
        df_store_sales
        .filter(F.col("product_id").isin([2, 4, 5]))
        .select("id")
    )
)

reconciled = (
    df_store_sales
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = reconciled.select("id", "sale_date", "units", "revenue")
