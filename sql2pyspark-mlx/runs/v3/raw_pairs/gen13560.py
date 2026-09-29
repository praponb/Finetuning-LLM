from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city").isin(["London", "Bangkok", "New York"]))
    .select("supplier_name", "city")
    .orderBy("city")
)
