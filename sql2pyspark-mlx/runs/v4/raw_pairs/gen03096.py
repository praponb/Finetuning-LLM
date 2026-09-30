from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("salary")
w2 = Window.orderBy("salary", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.partitionBy("dept_id").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("manager_id").orderBy("salary", "id")
w5 = Window.partitionBy("dept_id")
w6 = Window.partitionBy("dept_id", "job_title").orderBy(F.col("salary_avg_3").desc(), "id")

cleaned = (
    df_employees
    .filter((F.col("hire_date") >= F.lit("2023-03-14").cast("date")) & F.col("job_title").isNotNull())
    .select(
        "*",
        F.trim("level").alias("clean_level"),
        F.upper("job_title").alias("job_title_upper"),
    )
)

scored = (
    cleaned
    .select(
        "*",
        F.rank().over(w).alias("salary_rank"),
        F.avg("salary").over(w2).alias("salary_avg_3"),
        F.sum("salary").over(w3).alias("cumulative_salary"),
        F.lag("salary", 1).over(w4).alias("prev_salary"),
        F.avg("salary").over(w5).alias("dept_id_avg_salary"),
    )
)

latest = (
    scored
    .withColumn("row_num", F.row_number().over(w6))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    latest
    .rollup(
        F.when(F.col("salary_rank") >= 7, "LARGE").otherwise("SMALL").alias("salary_rank_tier"),
        F.date_trunc("year", "hire_date").alias("year_start"),
    )
    .agg(
        F.countDistinct("level").alias("distinct_levels"),
        F.sum(F.when(F.col("level") == "L3", F.col("salary_avg_3")).otherwise(0)).alias("l3_salary_avg_3"),
    )
    .orderBy(F.col("year_start").asc_nulls_last())
)
