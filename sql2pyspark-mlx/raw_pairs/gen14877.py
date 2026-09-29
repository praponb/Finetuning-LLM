from pyspark.sql import functions as F

kept_keys = (
    df_inventory
    .select("product_id")
    .subtract(
        df_inventory
        .filter(F.col("product_id").isin([4, 7, 9]))
        .select("product_id")
    )
)

verified = (
    df_inventory
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy("store_id", "product_id")
    .agg(
        F.bit_and("quantity").alias("quantity_bit_and"),
        F.percentile_approx("quantity", [0.25, 0.5, 0.75]).alias("quantity_quartiles"),
        F.count("*").alias("row_count"),
        F.countDistinct("product_id").alias("distinct_product_ids"),
    )
    .orderBy(F.col("distinct_product_ids").asc_nulls_first())
)
