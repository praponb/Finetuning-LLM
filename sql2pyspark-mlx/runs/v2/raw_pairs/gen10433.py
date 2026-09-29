from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(~(F.upper("supplier_name").like("%IAM%") & F.col("country").isin(["TH", "SG"])))
    .select(
        "id",
        "rating",
        "supplier_name",
        "city",
        F.substring("supplier_name", 1, 4).alias("supplier_name_prefix"),
    )
    .orderBy("supplier_name", "id")
)
