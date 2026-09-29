from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("city") == "Chiang Mai")
    .select("opened_date")
    .union(
        df_employees
        .filter(F.col("manager_id").isin([26, 33]))
        .select("hire_date")
    )
    .distinct()
)
