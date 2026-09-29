from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("level").orderBy("salary", "id")

enriched = (
    df_employees
    .filter(F.col("level").isin(["L5", "L3"]))
    .withColumn("prev_salary", F.lag("salary").over(w_part))
    .withColumn("event_seq", F.row_number().over(w_part))
)

result = (
    enriched
    .cube(
        F.date_trunc("YEAR", "hire_date").alias("year_start"),
        F.date_trunc("QUARTER", "hire_date").alias("quarter_start"),
        F.when(F.col("salary") >= 127000, "LARGE").otherwise("SMALL").alias("salary_tier"),
    )
    .agg(
        F.countDistinct("dept_id").alias("distinct_dept_ids"),
        F.round(F.sum("prev_salary") / F.nullif(F.count(F.when(F.col("job_title") == "Manager", 1)), F.lit(0)), 2).alias("prev_salary_per_manager"),
        F.sum(F.when(F.col("level") == "L1", F.col("salary"))).alias("l1_total_salary"),
        F.count(F.when(F.col("level") == "L1", 1)).alias("l1_count"),
        F.sum(F.when(F.col("level") == "L5", F.col("prev_salary"))).alias("l5_total_prev_salary"),
    )
    .filter(F.col("l5_total_prev_salary") > 0)
)
