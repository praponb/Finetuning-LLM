from pyspark.sql import functions as F

result = (
    df_departments
    .select("id", "location")
    .intersect(
        df_departments
        .filter(F.col("dept_name") == "Legal")
        .select("id", "location")
    )
)
