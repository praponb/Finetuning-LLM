from pyspark.sql import functions as F

remaining_keys = (
    df_products
    .select("brand", "supplier_id")
    .exceptAll(
        df_products
        .filter(F.col("category") == "Grocery")
        .select("brand", "supplier_id")
    )
)

reconciled = (
    df_products
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.brand") == F.col("k.brand")) & (F.col("s.supplier_id") == F.col("k.supplier_id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select(
        "product_name",
        "brand",
        "price",
        "cost",
        "launch_date",
        F.when(F.col("brand").rlike("^(Globex|Stark)$"), "PRIORITY").when(F.col("brand").rlike("^(Acme|Initech)$"), "OTHER_ZONE").otherwise("GROUP_B").alias("brand_zone"),
    )
)
