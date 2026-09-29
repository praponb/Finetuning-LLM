from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("hire_date", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

metrics = (
    df_employees
    .select(
        "id",
        "dept_id",
        "manager_id",
        "salary",
        "is_remote",
        F.avg("salary").over(w).alias("salary_avg_2"),
        F.sum("salary").over(w2).alias("cumulative_salary"),
    )
)

result = (
    metrics
    .rollup(
        F.when(F.col("cumulative_salary") >= 617513, "LARGE").otherwise("SMALL").alias("cumulative_salary_tier"),
        "dept_id",
        F.when(F.col("salary") >= 162000, "HIGH").otherwise("LOW").alias("salary_tier"),
    )
    .agg(
        F.min("salary_avg_2").alias("min_salary_avg_2"),
        F.max("salary_avg_2").alias("max_salary_avg_2"),
    )
    .orderBy(F.col("max_salary_avg_2").asc_nulls_first())
)
