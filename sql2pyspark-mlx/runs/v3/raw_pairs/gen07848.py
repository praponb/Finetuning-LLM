from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("level") == "L4")
    .groupBy(
        F.when(F.year("hire_date") < 2024, "before_2024").when(F.year("hire_date") == 2024, "2024").otherwise("after_2024").alias("hire_cohort"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("level"))).alias("level_values"),
        F.round(F.stddev("salary"), 2).alias("stddev_salary"),
    )
    .orderBy("hire_cohort")
)
