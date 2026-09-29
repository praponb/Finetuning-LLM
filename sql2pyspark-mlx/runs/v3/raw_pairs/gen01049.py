from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget").between(1530000, 2206000) | F.col("budget").between(1497000, 3441000))
    .select(
        "location",
        "dept_name",
        "budget",
        F.when(F.col("dept_name").isin(["Sales", "HR"]), "Core").otherwise("Other").alias("dept_name_group"),
    )
)
