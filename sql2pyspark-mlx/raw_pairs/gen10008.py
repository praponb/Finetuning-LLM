from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("name").isNotNull())
    .select("name")
    .subtract(
        df_departments
        .filter(F.col("dept_name").isNotNull())
        .select("dept_name")
    )
    .orderBy("name")
)
