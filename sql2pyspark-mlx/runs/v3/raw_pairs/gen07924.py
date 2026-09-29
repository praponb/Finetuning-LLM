from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating") >= 2.5)
    .select("city", "supplier_name")
    .subtract(
        df_suppliers
        .filter(~F.col("city").isin(["Tokyo", "Berlin", "Bangkok"]))
        .select("city", "supplier_name")
    )
)
