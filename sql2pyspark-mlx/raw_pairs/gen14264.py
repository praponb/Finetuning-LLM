from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("city") == "Tokyo") & F.col("city").isin(["Singapore", "Bangkok", "Tokyo"]))
    .select(
        "id",
        "city",
        "supplier_name",
        "country",
        F.concat("city", F.lit("/"), "supplier_name").alias("city_supplier_name"),
    )
)
