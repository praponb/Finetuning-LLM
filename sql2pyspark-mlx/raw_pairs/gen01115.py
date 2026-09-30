from pyspark.sql import functions as F

matched_keys = (
    df_shipments
    .select("order_id", "carrier")
    .intersect(
        df_shipments
        .filter(~F.col("status").isin(["IN_TRANSIT", "RETURNED", "LOST"]))
        .select("order_id", "carrier")
    )
)

kept_rows = (
    df_shipments
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")) & (F.col("s.carrier") == F.col("k.carrier")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select(
        "id",
        "order_id",
        "cost",
        "status",
        F.when(F.col("status").rlike("^(LOST|RETURNED)$"), "GROUP_B").otherwise("CORE").alias("status_zone"),
    )
)
