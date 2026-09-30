from pyspark.sql import functions as F

result = (
    df_departments
    .select("location")
    .union(df_suppliers.select("city"))
)
