from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.length("supplier_name") >= 13) & (F.col("city") != "Chiang Mai"))
    .select(
        "id",
        "country",
        "city",
        "supplier_name",
        F.when(F.col("city") == "Berlin", 1).otherwise(0).alias("is_berlin"),
    )
    .orderBy("supplier_name", F.col("city").desc())
)
