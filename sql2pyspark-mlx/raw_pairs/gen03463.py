from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(F.when(F.col("dept_name").isin(["Marketing", "Sales"]), "priority").otherwise("other").alias("dept_name_group"))
    .pivot("location", ["New York", "London", "Chiang Mai", "Singapore"])
    .agg(F.sum("budget"))
    .orderBy("dept_name_group")
)
