from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("level").isin(["L5", "L1", "L2"]))
    .groupBy("is_remote")
    .agg(
        F.max("salary").alias("max_salary"),
        F.concat_ws(", ", F.sort_array(F.collect_set("level"))).alias("level_values"),
        F.count("*").alias("total_count"),
    )
    .orderBy("max_salary")
)
