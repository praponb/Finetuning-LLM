from pyspark.sql import functions as F

result = (
    df_departments
    .filter(~(F.col("dept_name").isin(["Engineering", "HR"]) & F.col("location").isin(["Singapore", "Chiang Mai"])))
    .groupBy("dept_name")
    .agg(
        (F.max("budget") - F.min("budget")).alias("budget_range"),
        F.sort_array(F.collect_set("dept_name")).alias("dept_name_list"),
    )
    .filter(F.col("budget_range") >= 31)
    .orderBy("dept_name")
    .limit(10)
)
