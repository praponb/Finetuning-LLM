from pyspark.sql import functions as F

result = (
    df_employees
    .filter(~((F.length("email") >= 11) & (F.col("manager_id").isin([18, 36]) | ~F.col("is_remote"))))
    .select("id", "salary", "level", "manager_id")
    .orderBy("salary", "id")
)
