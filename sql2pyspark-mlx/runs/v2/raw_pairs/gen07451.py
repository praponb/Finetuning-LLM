from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country") != "TH")
    .select(
        "supplier_name",
        "city",
        "country",
        F.concat_ws("-", "supplier_name", "city").alias("supplier_name_city_key"),
        F.trim("city").alias("clean_city"),
    )
)
