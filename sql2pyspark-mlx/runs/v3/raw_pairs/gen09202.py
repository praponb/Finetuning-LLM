from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") > 2776000)
    .groupBy("dept_name")
    .agg(
        F.sort_array(F.collect_set("location")).alias("location_list"),
        (F.max("budget") - F.min("budget")).alias("budget_range"),
    )
    .filter(F.col("budget_range") > 43)
    .select("dept_name", "location_list", "budget_range")
)
