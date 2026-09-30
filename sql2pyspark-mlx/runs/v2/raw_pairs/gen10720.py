from pyspark.sql import functions as F

result = (
    df_customers
    .select("city")
    .subtract(df_suppliers.filter(F.col("country") == "US").select("city"))
)
