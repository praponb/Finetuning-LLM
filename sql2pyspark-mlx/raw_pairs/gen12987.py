from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isin(["HR", "Sales"]))
    .select("location")
    .union(df_customers.filter(F.col("age") >= 56).select("email"))
    .orderBy("location")
)
