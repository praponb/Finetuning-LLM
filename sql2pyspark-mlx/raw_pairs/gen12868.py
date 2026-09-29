from pyspark.sql import functions as F

matched_keys = (
    df_store_sales
    .select("id", "store_id")
    .exceptAll(
        df_store_sales
        .filter(F.col("units") > 30)
        .select("id", "store_id")
    )
)

reconciled = (
    df_store_sales
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.store_id") == F.col("k.store_id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select("id", "store_id", "product_id", "sale_date", "units")
    .orderBy(F.col("units").desc_nulls_last())
)
