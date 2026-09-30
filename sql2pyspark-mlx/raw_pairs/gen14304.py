from pyspark.sql import functions as F

kept_keys = (
    df_shipments
    .select("order_id", "id")
    .exceptAll(
        df_shipments
        .filter(F.col("cost").between(27, 69))
        .select("order_id", "id")
    )
)

verified = (
    df_shipments
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = verified.select("carrier", "shipped_date", "delivered_date", "cost")
