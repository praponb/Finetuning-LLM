from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())
w_recent = Window.partitionBy("dept_name").orderBy("hire_date", "id").rowsBetween(-2, Window.currentRow)
w_hist = Window.orderBy("last3_min_salary", "id")
w4 = Window.orderBy("last3_sum_salary", "id")
w5 = Window.partitionBy("last3_sum_salary_tier").orderBy(F.col("distinct_ids_4").desc())

valid = (
    df_departments
    .filter(F.col("dept_name").isNotNull())
    .select(
        "*",
        F.when(F.col("dept_name") == "Sales", 1).otherwise(0).alias("is_sales"),
    )
)

with_last = (
    valid
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.dept_name", F.col("c.salary"), F.col("c.hire_date"))
)

metrics = (
    with_last
    .select(
        "id",
        "salary",
        "hire_date",
        F.sum("salary").over(w_recent).alias("last3_sum_salary"),
        F.max("salary").over(w_recent).alias("last3_max_salary"),
        F.min("salary").over(w_recent).alias("last3_min_salary"),
    )
)

filtered = (
    metrics
    .filter(F.col("hire_date") >= "2025-08-06")
    .withColumn("last3_sum_salary_band", F.when(F.col("last3_sum_salary") >= 749000, "High").when(F.col("last3_sum_salary") >= 149000, "Medium").otherwise("Low"))
)

metrics_departments = (
    filtered
    .filter(F.month("hire_date").isin([6, 7, 9]))
    .select(
        "*",
        F.lead("last3_min_salary").over(w_hist).alias("next_last3_min_salary"),
        F.lead("last3_max_salary").over(w_hist).alias("next_last3_max_salary"),
        F.row_number().over(w_hist).alias("row_seq"),
        F.row_number().over(w4).alias("event_seq"),
    )
)

kept = (
    metrics_departments
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.col("x.hire_date") < F.col("s.hire_date")), "left_semi")
)

eligible = (
    kept
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.col("x.is_remote")), "left_semi")
)

result = (
    eligible
    .groupBy(
        F.when(F.col("last3_sum_salary") >= 669000, "PREMIUM").otherwise("STANDARD").alias("last3_sum_salary_tier"),
        F.date_trunc("MONTH", "hire_date").alias("activity_month"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("id").alias("distinct_ids_2"),
        F.percentile_approx("event_seq", [0.25, 0.5, 0.75]).alias("event_seq_quartiles"),
        F.countDistinct("id").alias("distinct_ids_3"),
        F.countDistinct("id").alias("distinct_ids_4"),
    )
    .withColumn("distinct_ids_4_dense_rank", F.dense_rank().over(w5))
    .orderBy("distinct_ids_3")
)
