from pyspark.sql import functions as F

kept_keys = (
    df_inventory
    .select("id")
    .exceptAll(
        df_inventory
        .filter(F.col("quantity") <= 94)
        .select("id")
    )
)

kept_rows = (
    df_inventory
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy("product_id", "store_id")
    .agg(
        F.percentile_approx("quantity", [0.25, 0.5, 0.75]).alias("quantity_quartiles"),
        F.percentile_approx("quantity", [0.25, 0.5, 0.75]).alias("quantity_quartiles_2"),
        F.percentile_approx("quantity", [0.25, 0.5, 0.75]).alias("quantity_quartiles_3"),
    )
)
