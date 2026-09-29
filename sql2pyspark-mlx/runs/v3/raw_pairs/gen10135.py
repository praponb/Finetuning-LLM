from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name") == "Marketing")
    .groupBy("dept_name")
    .agg(F.count(F.when(F.col("dept_name") == "Sales", 1)).alias("sales_count"))
    .filter(F.col("sales_count") > 9)
    .select("dept_name", "sales_count")
    .orderBy(F.col("dept_name").desc(), "sales_count")
)
