from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget").between(1269000, 3806000) & ((F.col("budget") >= 3888000) | (F.col("budget") <= 3855000)))
    .select(
        "dept_name",
        "budget",
        "location",
        F.initcap(F.trim("location")).alias("location_title"),
    )
)
