from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isin(["Finance", "Sales", "Marketing"]))
    .select("dept_name")
    .distinct()
    .orderBy("dept_name")
    .limit(5)
)
