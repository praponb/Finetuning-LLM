from pyspark.sql import functions as F

kept_keys = (
    df_order_items
    .select("product_id", "id")
    .exceptAll(
        df_order_items
        .filter(F.col("product_id").isNull())
        .select("product_id", "id")
    )
)

reconciled = (
    df_order_items
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy("order_id")
    .agg(
        F.bit_and("quantity").alias("quantity_bit_and"),
        F.countDistinct("order_id").alias("distinct_order_ids"),
        F.corr("quantity", "unit_price").alias("corr_quantity_unit_price"),
    )
    .filter(F.col("distinct_order_ids") > 1)
    .orderBy(F.col("order_id").desc_nulls_first())
)
