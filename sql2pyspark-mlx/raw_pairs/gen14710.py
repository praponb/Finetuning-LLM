from pyspark.sql import functions as F

matched_keys = (
    df_orders
    .select("id", "store_id")
    .exceptAll(
        df_orders
        .filter(F.col("amount").between(680, 1220))
        .select("id", "store_id")
    )
)

reconciled = (
    df_orders
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.store_id") == F.col("k.store_id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select("id", "store_id", "order_date", "amount", "shipping_fee")
    .orderBy(F.col("store_id").asc_nulls_last())
)
