from pyspark.sql import functions as F

remaining_keys = (
    df_shipments
    .select("carrier", "id")
    .intersectAll(
        df_shipments
        .filter(F.col("status") == "DELIVERED")
        .select("carrier", "id")
    )
)

kept_rows = (
    df_shipments
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.carrier") == F.col("k.carrier")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select(
        "id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        F.when(F.col("carrier").rlike("^(FedEx|Kerry)$"), "GROUP_B").when(F.col("carrier").rlike("^(DHL|Flash)$"), "GROUP_A").otherwise("CORE").alias("carrier_zone"),
    )
)
