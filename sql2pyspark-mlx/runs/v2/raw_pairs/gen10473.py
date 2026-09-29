from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(((F.col("city") != "London") & F.col("supplier_name").like("E%")) | (F.col("rating") >= 2.2))
    .select(
        "id",
        "city",
        "supplier_name",
        "country",
        F.upper("city").alias("city_upper"),
        F.upper("city").alias("city_upper_2"),
    )
)
