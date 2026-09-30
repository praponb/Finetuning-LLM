from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())
w2 = Window.orderBy("budget", "id")
w3 = Window.partitionBy("employee_level")
w4 = Window.partitionBy("employee_level").orderBy("budget", "id")

with_latest = (
    df_departments
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.budget", F.col("c.level").alias("employee_level"))
)

enriched = (
    with_latest
    .select(
        "id",
        "budget",
        "employee_level",
        F.row_number().over(w2).alias("seq_no"),
        F.round((F.col("budget") / F.sum("budget").over(w3)), 4).alias("budget_share"),
        F.ntile(10).over(w4).alias("budget_bucket"),
    )
)

base = (
    enriched
    .filter(F.col("employee_level").isNotNull() & F.col("employee_level").isin(["L4", "L5", "L1"]))
    .withColumn("budget_bucket_minus_budget_share", (F.col("budget_bucket") - F.col("budget_share")))
)

not_flagged = (
    base
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.col("c.dept_id").isin([22, 24, 27, 36])), "left_anti")
)

latest_match = (
    not_flagged
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.hire_date").alias("employee_hire_date"))
)

result = (
    latest_match
    .groupBy(
        F.when(F.col("budget") >= 3287000, "LARGE").otherwise("SMALL").alias("budget_tier"),
    )
    .agg(
        F.size(F.collect_set("employee_level")).alias("num_employee_levels"),
        F.sum("seq_no").alias("total_seq_no"),
        F.stddev_samp("budget").alias("budget_stddev"),
    )
    .filter(F.col("num_employee_levels") > 1)
    .orderBy(F.col("budget_tier").desc())
)
