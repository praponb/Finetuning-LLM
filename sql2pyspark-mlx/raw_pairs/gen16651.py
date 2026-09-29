from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("quarter_start").orderBy(F.col("l5_events").desc())

result = (
    df_employees
    .groupBy(
        F.date_trunc("QUARTER", "hire_date").alias("quarter_start"),
        F.when(F.col("salary") >= 122000, "PREMIUM").otherwise("STANDARD").alias("salary_tier"),
    )
    .agg(
        F.count_if(F.col("job_title") == "Intern").alias("intern_events"),
        F.min("salary").alias("min_salary"),
        F.countDistinct("level").alias("distinct_levels"),
        F.max(F.when(F.col("level") == "L2", F.col("salary"))).alias("l2_max_salary"),
        F.round(F.avg("salary"), 2).alias("avg_salary"),
        F.count_if(F.col("level") == "L5").alias("l5_events"),
    )
    .withColumn("l5_events_rank", F.rank().over(w))
    .orderBy(F.col("avg_salary").asc_nulls_last())
)
