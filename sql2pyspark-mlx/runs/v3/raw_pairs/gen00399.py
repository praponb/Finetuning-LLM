from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name") == "Finance")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("dept_name"))).alias("dept_name_values"),
    )
    .orderBy("dept_name_values")
)
