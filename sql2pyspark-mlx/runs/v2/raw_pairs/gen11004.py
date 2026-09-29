from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city").isin(["New York", "Bangkok"]))
    .select("supplier_name")
    .unionByName(df_suppliers.filter(F.col("rating") > 3.3).select("supplier_name"))
    .orderBy(F.col("supplier_name").desc())
)
