from pyspark.sql import functions as F

matched_keys = (
    df_store_sales
    .select("store_id", "product_id")
    .intersect(
        df_store_sales
        .filter(F.col("sale_date") < "2023-06-04")
        .select("store_id", "product_id")
    )
)

verified = (
    df_store_sales
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")) & (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

result = (
    verified
    .select("id", "store_id", "product_id", "sale_date", "units")
    .orderBy(F.col("product_id").desc_nulls_last())
)
