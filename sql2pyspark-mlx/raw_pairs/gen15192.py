from pyspark.sql import functions as F

kept_keys = (
    df_store_sales
    .select("product_id", "id")
    .subtract(
        df_store_sales
        .filter(F.year("sale_date") >= 2025)
        .select("product_id", "id")
    )
)

reconciled = (
    df_store_sales
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select("id", "store_id", "product_id", "units", "revenue")
    .orderBy(F.col("store_id").desc())
)
