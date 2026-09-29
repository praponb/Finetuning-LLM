from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("email").isNull())
    .groupBy(
        "manager_id",
        F.quarter("hire_date").alias("hire_quarter"),
        F.month("hire_date").alias("hire_month"),
    )
    .agg(
        F.sum("salary").alias("total_salary"),
        F.concat_ws(", ", F.sort_array(F.collect_set("level"))).alias("level_values"),
    )
    .orderBy(F.col("manager_id").desc(), "hire_quarter", "hire_month")
    .limit(50)
)
