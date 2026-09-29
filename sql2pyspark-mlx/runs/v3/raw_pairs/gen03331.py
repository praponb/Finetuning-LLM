from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("supplier_name").like("G%") & (F.col("rating") > 2.5)) | ~F.col("country").isin(["DE", "UK", "TH"]))
    .select(
        "id",
        "supplier_name",
        F.concat_ws("/", "supplier_name", "city").alias("supplier_name_city_key"),
        F.coalesce("city", F.lit("unassigned")).alias("city_filled"),
    )
)
