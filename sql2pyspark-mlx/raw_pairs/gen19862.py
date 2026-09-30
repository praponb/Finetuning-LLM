from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy(F.col("budget").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())
w_grp = Window.partitionBy("location").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("location")

deduped = (
    df_departments
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "location")
)

with_last = (
    deduped
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.location", F.col("c.salary"))
)

not_flagged = (
    with_last
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.level") != "L5"), "left_semi")
)

enriched = (
    not_flagged
    .select(
        "*",
        F.sum("salary").over(w_grp).alias("running_sum_salary"),
        F.count("salary").over(w_grp).alias("running_count_salary"),
        F.min("salary").over(w_grp).alias("running_min_salary"),
        F.avg("salary").over(w_grp).alias("running_avg_salary"),
        F.round((F.col("salary") / F.sum("salary").over(w4)), 4).alias("salary_share"),
    )
)

result = (
    enriched
    .rollup(
        F.when(F.col("running_count_salary") >= 14, "HIGH").otherwise("LOW").alias("running_count_salary_tier"),
        "location",
    )
    .agg(
        F.percentile_approx("running_sum_salary", 0.9).alias("running_sum_salary_p90"),
        F.size(F.collect_set("location")).alias("num_locations"),
        F.count(F.when(F.col("location") == "New York", 1)).alias("new_york_count"),
    )
    .orderBy(F.col("new_york_count").desc_nulls_first())
)
