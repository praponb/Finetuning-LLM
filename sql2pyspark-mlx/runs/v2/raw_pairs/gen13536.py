from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.lower("email").like("%any%"))
    .select("id", "full_name")
    .intersect(
        df_employees
        .filter(F.col("dept_id").isin([10, 12, 28, 39]))
        .select("id", "full_name")
    )
    .orderBy("id")
)
