from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city").isin(["Tokyo", "London", "Bangkok"]) & F.col("supplier_name").isNotNull())
    .select(
        "id",
        "country",
        "city",
        F.coalesce("city", F.lit("N/A")).alias("city_filled"),
    )
)
