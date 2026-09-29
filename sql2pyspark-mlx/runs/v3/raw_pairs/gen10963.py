from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("dept_name").isin(["Marketing", "Finance"]), "priority").otherwise("other").alias("dept_name_group"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("dept_name"))).alias("dept_name_values"),
    )
    .orderBy("dept_name_values")
)
