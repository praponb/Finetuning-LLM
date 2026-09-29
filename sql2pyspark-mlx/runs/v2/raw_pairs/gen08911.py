from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("dept_name").isNotNull() & (F.col("budget") > 2518000)) | F.col("dept_name").isin(["HR", "Legal", "Marketing"]))
)
