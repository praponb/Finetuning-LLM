from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isin(["Chiang Mai", "Tokyo"]))
    .select("dept_name")
    .intersect(
        df_departments
        .filter(F.col("location").isin(["Tokyo", "Berlin"]))
        .select("dept_name")
    )
    .orderBy(F.col("dept_name").desc())
)
