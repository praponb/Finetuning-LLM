from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("full_name").isNotNull())
    .select("id", "full_name", "level")
    .unionByName(
        df_employees
        .filter(F.col("dept_id").isin([1, 23, 25, 39]))
        .select("id", "full_name", "level")
    )
    .distinct()
    .orderBy("full_name")
)
