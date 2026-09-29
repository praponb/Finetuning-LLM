from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("location") == "Bangkok") & (F.col("budget") <= 1576000) & F.col("budget").isNotNull())
    .select(
        "id",
        "dept_name",
        "budget",
        F.initcap(F.trim("dept_name")).alias("dept_name_title"),
    )
)
