from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("salary_tier").orderBy(F.col("distinct_levels").desc())

joined = (
    df_employees
    .alias("s")
    .join(df_departments.alias("d"), F.col("s.dept_id") == F.col("d.id"), "left")
    .filter(F.col("d.location") == "Bangkok")
    .select(
        "s.id",
        "s.email",
        "s.dept_id",
        "s.manager_id",
        "s.salary",
        "s.hire_date",
        "s.is_remote",
        "s.level",
        F.col("d.budget"),
        F.col("d.location"),
    )
)

result = (
    joined
    .groupBy(
        F.when(F.col("salary") >= 120000, "PREMIUM").otherwise("STANDARD").alias("salary_tier"),
        F.date_trunc("YEAR", "hire_date").alias("year_start"),
        F.when(F.col("budget") >= 3145000, "PREMIUM").otherwise("STANDARD").alias("budget_tier"),
    )
    .agg(
        F.stddev_samp("salary").alias("salary_stddev"),
        F.percentile_approx("budget", [0.25, 0.5, 0.75]).alias("budget_quartiles"),
        F.countDistinct("level").alias("distinct_levels"),
    )
    .withColumn("distinct_levels_dense_rank", F.dense_rank().over(w))
    .orderBy(F.col("budget_tier").asc_nulls_first())
)
