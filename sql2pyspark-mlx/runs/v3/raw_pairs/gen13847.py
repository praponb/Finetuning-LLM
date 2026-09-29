from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country").isin(["JP", "SG"]) | F.col("rating").between(2.3, 4.0))
    .select(
        "id",
        "country",
        "supplier_name",
        "city",
        F.concat("supplier_name", F.lit(" "), "city").alias("supplier_name_city"),
        F.upper(F.trim("supplier_name")).alias("supplier_name_normalized"),
    )
    .orderBy("id")
)
