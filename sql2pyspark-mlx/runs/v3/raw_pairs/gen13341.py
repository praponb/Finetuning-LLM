from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country") == "TH")
    .select(
        "id",
        "supplier_name",
        "city",
        "rating",
        F.when(F.col("country").isin(["JP", "US"]), "Group A").otherwise("Other").alias("country_group"),
    )
    .orderBy("supplier_name")
)
