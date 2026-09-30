from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isNull())
    .groupBy("location")
    .pivot("dept_name", ["Legal", "Finance", "Sales", "Marketing"])
    .agg(F.count("budget"))
    .orderBy("location")
)
