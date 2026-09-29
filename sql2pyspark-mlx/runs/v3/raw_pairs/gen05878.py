from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city").isin(["Tokyo", "New York"]))
    .select("id", "supplier_name")
    .intersect(
        df_suppliers
        .filter(F.col("country").isin(["UK", "DE"]))
        .select("id", "supplier_name")
    )
    .orderBy("id")
)
