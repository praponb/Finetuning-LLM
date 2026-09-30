from pyspark.sql import functions as F

remaining_keys = (
    df_store_sales
    .select("product_id")
    .subtract(
        df_store_sales
        .filter(F.col("store_id").isin([1, 2, 5, 6]))
        .select("product_id")
    )
)

kept_rows = (
    df_store_sales
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

result = kept_rows.select("id", "store_id", "product_id", "units", "revenue")
