from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("employee_job_title").orderBy(F.col("budget").desc_nulls_last(), "id")

base = (
    df_departments
    .filter(F.col("dept_name") == "Legal")
    .withColumn("location_length", F.length("location"))
)

with_last = (
    base
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.job_title").alias("employee_job_title"))
)

ranked = (
    with_last
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
)

result = (
    ranked
    .withColumn("location_length_tier", F.when(F.col("location_length") >= 68, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["location_length_tier", "dept_name"], ["location_length_tier"]],
        "location_length_tier",
        "dept_name",
    )
    .agg(
        F.count(F.when(F.col("location") == "Chiang Mai", 1)).alias("chiang_mai_count"),
        F.percentile_approx("budget", [0.25, 0.5, 0.75]).alias("budget_quartiles"),
        F.grouping("location_length_tier").alias("grouping_level"),
    )
    .filter(F.col("chiang_mai_count") < 1)
)
