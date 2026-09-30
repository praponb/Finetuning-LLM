from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.length("full_name") >= 7)
    .select("hire_date")
    .unionByName(
        df_employees
        .filter(F.col("full_name").isNotNull())
        .select("hire_date")
    )
    .distinct()
)
