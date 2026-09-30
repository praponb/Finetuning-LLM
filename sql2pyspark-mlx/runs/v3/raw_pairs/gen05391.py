from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isNotNull())
    .select("location")
    .union(df_suppliers.select("city"))
    .distinct()
)
