from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("location") == "Bangkok") | F.col("dept_name").isin(["Engineering", "Finance", "Sales"]))
    .groupBy("dept_name")
    .agg(
        F.countDistinct("budget").alias("unique_dept_names"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("row_count") >= 5)
)
