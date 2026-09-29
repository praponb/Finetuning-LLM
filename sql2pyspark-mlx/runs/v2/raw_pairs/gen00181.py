from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("full_name").isNotNull())
    .select("dept_id")
    .unionByName(
        df_employees
        .filter(F.col("job_title").isin(["Director", "Designer"]))
        .select("dept_id")
    )
    .orderBy("dept_id")
)
