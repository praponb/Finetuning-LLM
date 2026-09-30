from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.length("email") > 9) & (F.col("manager_id").isNull() | F.col("dept_id").isin([28, 30, 34])))
    .groupBy("is_remote")
    .agg(
        F.sort_array(F.collect_set("level")).alias("level_list"),
        (F.max("salary") - F.min("salary")).alias("salary_range"),
    )
    .orderBy("salary_range")
)
