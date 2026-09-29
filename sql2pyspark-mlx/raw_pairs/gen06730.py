from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("dept_id", "level")
    .agg(
        F.sort_array(F.collect_set("level")).alias("level_list"),
        F.sum(F.when(F.col("level") == "L1", F.col("salary")).otherwise(0)).alias("l1_salary"),
    )
    .filter(F.col("l1_salary") < 76)
    .select("dept_id", "level", "level_list", "l1_salary")
)
