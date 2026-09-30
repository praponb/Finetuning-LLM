from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("dept_id") == 40)
    .select("hire_date", "full_name")
    .unionByName(
        df_employees
        .filter(F.col("level").isin(["L1", "L3", "L5"]))
        .select("hire_date", "full_name")
    )
    .distinct()
)
