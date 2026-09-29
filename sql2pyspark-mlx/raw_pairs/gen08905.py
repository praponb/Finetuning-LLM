from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("manager_id").isin([24, 40]))
    .select("level")
    .unionByName(
        df_employees
        .filter(F.col("dept_id").isin([1, 36, 40]))
        .select("level")
    )
    .distinct()
    .orderBy("level")
)
