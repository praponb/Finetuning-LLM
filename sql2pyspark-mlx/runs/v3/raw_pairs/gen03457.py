from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("location", "dept_name")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("dept_name"))).alias("dept_name_values"),
        F.count(F.when(F.col("dept_name") == "Legal", 1)).alias("legal_count"),
    )
    .filter(F.col("legal_count") > 5)
    .select("location", "dept_name", "dept_name_values", "legal_count")
)
