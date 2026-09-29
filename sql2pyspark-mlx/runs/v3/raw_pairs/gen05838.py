from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(~F.col("supplier_name").like("%Import%") & (F.col("rating") <= 4.1))
    .select(
        "id",
        "supplier_name",
        "rating",
        "city",
        F.concat("country", F.lit(" "), "supplier_name").alias("country_supplier_name"),
    )
)
