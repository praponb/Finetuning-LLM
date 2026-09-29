from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating").isNotNull() | F.col("supplier_name").isNull())
    .select(
        "id",
        "rating",
        F.concat("supplier_name", F.lit(" "), "city").alias("supplier_name_city"),
    )
)
