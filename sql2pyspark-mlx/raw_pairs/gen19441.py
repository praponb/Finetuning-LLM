from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("city").isin(["Tokyo", "London", "Berlin"]) & (F.col("city") != "Berlin")) | (F.col("rating") <= 2.4))
    .select(
        "id",
        "rating",
        "supplier_name",
        "country",
        F.length("supplier_name").alias("supplier_name_length"),
    )
    .orderBy("country", "id")
)
