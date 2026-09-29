from pyspark.sql import functions as F

filtered = (
    df_products
    .filter((F.col("launch_date") > F.to_date(F.lit("2023-09-28"))) & F.col("brand").isNotNull() & (F.size("tags") >= 1))
    .select(
        "*",
        F.when(F.col("brand").isin(["Umbrella", "Acme"]), "Core").otherwise("Other").alias("brand_group"),
        F.coalesce("brand", F.lit("UNKNOWN")).alias("brand_filled"),
    )
)

result = (
    filtered
    .select("supplier_id", "brand")
    .exceptAll(
        df_products
        .filter(F.col("launch_date").between("2023-07-01", "2023-10-01"))
        .select("supplier_id", "brand")
    )
)
