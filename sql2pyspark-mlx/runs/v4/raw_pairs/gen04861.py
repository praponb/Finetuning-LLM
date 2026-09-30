from pyspark.sql import functions as F

result = (
    df_departments
    .filter(~(((F.col("budget") < 3968000) & (F.col("dept_name") != "Sales")) | F.col("budget").between(2476000, 2687000)))
    .select(
        "location",
        "dept_name",
        F.concat("location", F.lit(" "), "dept_name").alias("location_dept_name"),
    )
)
