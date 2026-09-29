from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("rating") <= 3.7) & F.col("country").isin(["UK", "TH"]) & (F.col("rating") >= 1.9))
    .select(
        "id",
        "supplier_name",
        "city",
        F.concat("supplier_name", F.lit("/"), "city").alias("supplier_name_city"),
    )
    .orderBy("supplier_name")
)
