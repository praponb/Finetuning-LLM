from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("city") == "Chiang Mai")
    .select("opened_date")
    .subtract(df_employees.select("hire_date"))
)
