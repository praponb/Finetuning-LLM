from pyspark.sql import functions as F

matched_keys = (
    df_shipments
    .select("carrier")
    .intersect(
        df_shipments
        .filter(F.col("cost") > 50)
        .select("carrier")
    )
)

reconciled = (
    df_shipments
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.carrier") == F.col("k.carrier")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select("id", "carrier", "delivered_date", "cost")
    .orderBy(F.col("id").desc_nulls_first())
)
