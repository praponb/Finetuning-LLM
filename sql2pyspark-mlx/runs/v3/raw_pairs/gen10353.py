from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("salary") >= 145000)
    .select("dept_id", "manager_id")
    .unionByName(
        df_employees
        .filter(F.col("level").isin(["L3", "L4", "L5"]))
        .select("dept_id", "manager_id")
    )
    .orderBy("dept_id")
)
