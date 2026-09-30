from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isin(["Engineering", "Legal"]))
    .select("dept_name", "location")
    .unionByName(
        df_departments
        .filter(F.col("location") != "Bangkok")
        .select("dept_name", "location")
    )
    .distinct()
    .orderBy("location")
)
