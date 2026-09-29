from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city").isin(["Tokyo", "Bangkok"]))
    .select(
        "id",
        "supplier_name",
        F.upper("supplier_name").alias("supplier_name_upper"),
    )
)
