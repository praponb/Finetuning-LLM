from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("is_remote"))
    .select("level")
    .unionByName(
        df_employees
        .filter(F.col("dept_id").isin([13, 18, 23, 37]))
        .select("level")
    )
    .distinct()
)
