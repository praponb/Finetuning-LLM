from pyspark.sql import functions as F

result = (
    df_employees
    .rollup("manager_id", "is_remote")
    .agg(
        F.max("hire_date").alias("latest_hire_date"),
        F.concat_ws(", ", F.sort_array(F.collect_set("level"))).alias("level_values"),
    )
    .orderBy("latest_hire_date")
)
