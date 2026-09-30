from pyspark.sql import functions as F

kept_keys = (
    df_order_items
    .select("order_id", "id")
    .subtract(
        df_order_items
        .filter(F.col("quantity") != 7)
        .select("order_id", "id")
    )
)

reconciled = (
    df_order_items
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy("order_id")
    .agg(
        F.corr("unit_price", "quantity").alias("corr_unit_price_quantity"),
        F.corr("unit_price", "quantity").alias("corr_unit_price_quantity_2"),
        F.sum("unit_price").alias("total_unit_price"),
    )
    .filter(F.col("total_unit_price") >= 0)
    .orderBy(F.col("total_unit_price").asc_nulls_first())
)
