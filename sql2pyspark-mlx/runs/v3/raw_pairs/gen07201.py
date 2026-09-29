from pyspark.sql import functions as F

result = (
    df_customers
    .select("city")
    .subtract(df_suppliers.filter(F.col("city") != "Bangkok").select("city"))
    .orderBy(F.col("city").desc())
)
