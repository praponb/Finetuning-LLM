from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("salary", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("job_title", "dept_id").orderBy("salary", "id").rowsBetween(-3, -1)
w_grp = Window.partitionBy("job_title").orderBy("hire_date", "id")
w4 = Window.partitionBy("job_title").orderBy(F.col("event_seq").desc(), "id")
w5 = Window.partitionBy("year_start").orderBy(F.col("l2_events").desc())

enriched = (
    df_employees
    .filter(F.col("email").like("%@startup.io"))
    .withColumn("salary_avg_6", F.avg("salary").over(w))
    .withColumn("salary_avg_3_prior", F.avg("salary").over(w2))
    .withColumn("event_seq", F.row_number().over(w_grp))
)

latest = (
    enriched
    .withColumn("row_num", F.row_number().over(w4))
    .filter(F.col("row_num") == 1)
)

kept_keys = (
    latest
    .select("manager_id", "job_title")
    .intersect(
        df_employees
        .filter(F.length("email") < 14)
        .select("manager_id", "job_title")
    )
)

kept_rows = (
    latest
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.manager_id") == F.col("k.manager_id")) & (F.col("s.job_title") == F.col("k.job_title")), "inner")
    .select("s.*")
)

enriched_employees = (
    kept_rows
    .alias("src")
    .join(df_departments.alias("b"), F.col("src.dept_id") == F.col("b.id"), "left")
    .select("src.*", F.col("b.budget"))
)

joined = (
    enriched_employees
    .alias("a")
    .join(df_employees.alias("b"), F.col("a.manager_id").eqNullSafe(F.col("b.id")), "left")
    .select("a.*", F.col("b.level").alias("employee_level"))
)

stats = (
    joined
    .groupBy(F.date_trunc("year", "hire_date").alias("year_start"), "level")
    .agg(
        F.percentile_approx("salary_avg_6", [0.25, 0.5, 0.75]).alias("salary_avg_6_quartiles"),
        F.count_if(F.col("level") == "L2").alias("l2_events"),
    )
    .withColumn("l2_events_rank", F.rank().over(w5))
)

result = (
    stats
    .select("year_start", "level", "salary_avg_6_quartiles", "l2_events_rank")
    .orderBy(F.col("year_start").desc(), "level")
)
