from pyspark.sql import functions as F

matched_keys = (
    df_inventory
    .select("id")
    .subtract(
        df_inventory
        .filter(F.col("quantity") >= 140)
        .select("id")
    )
)

kept_rows = (
    df_inventory
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = kept_rows.select("id", "store_id", "product_id")
