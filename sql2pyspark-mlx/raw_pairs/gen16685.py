from pyspark.sql import functions as F

kept_keys = (
    df_shipments
    .select("status", "id")
    .subtract(
        df_shipments
        .filter(F.col("cost").between(79, 81))
        .select("status", "id")
    )
)

reconciled = (
    df_shipments
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        F.when(F.col("carrier").rlike("^(Flash|FedEx)$"), "CORE").otherwise("EXTENDED").alias("carrier_zone"),
    )
    .orderBy("delivered_date", F.col("id").desc())
)
