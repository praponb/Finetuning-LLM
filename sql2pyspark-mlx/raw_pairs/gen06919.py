from pyspark.sql import functions as F

kept_keys = (
    df_suppliers
    .select("id")
    .exceptAll(
        df_suppliers
        .filter(F.col("city").isin(["Bangkok", "New York"]))
        .select("id")
    )
)

reconciled = (
    df_suppliers
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select("id", "supplier_name", "country")
    .orderBy("country", "id")
)
