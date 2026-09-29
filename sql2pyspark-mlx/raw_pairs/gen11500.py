from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy("hire_date_month_end", "id")
w2 = Window.partitionBy("manager_id").orderBy(F.col("hire_date_month_end").desc(), "id")
w3 = Window.partitionBy("manager_id").orderBy("employee_hire_date", "id")
w4 = Window.orderBy("hire_date_month_end", "id")

filtered = (
    df_employees
    .filter(F.col("full_name").like("F%"))
    .select(
        "id",
        "full_name",
        "email",
        "manager_id",
        "job_title",
        "hire_date",
        "is_remote",
        F.last_day("hire_date").alias("hire_date_month_end"),
    )
)

enriched = (
    filtered
    .alias("s")
    .join(df_employees.alias("b"), F.col("s.manager_id") == F.col("b.id"), "inner")
    .filter(F.col("b.salary") > 117000)
    .select(
        "s.id",
        "s.full_name",
        "s.email",
        "s.manager_id",
        "s.hire_date",
        "s.hire_date_month_end",
        F.col("b.hire_date").alias("employee_hire_date"),
    )
)

scored = (
    enriched
    .select(
        "*",
        F.row_number().over(w).alias("row_seq"),
        F.row_number().over(w2).alias("row_seq_2"),
        F.row_number().over(w3).alias("event_seq"),
        F.row_number().over(w4).alias("seq_no"),
    )
)

latest = (
    scored
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

result = (
    latest
    .alias("e")
    .join(df_employees.alias("p"), F.col("e.manager_id") == F.col("p.id"), "left")
    .cube(F.col("p.job_title").alias("job_title"))
    .agg(
        F.count("*").alias("cnt"),
        F.sum(F.col("e.row_seq_2")).alias("total_row_seq_2"),
        F.max(F.col("e.seq_no")).alias("max_seq_no"),
        F.grouping_id().alias("gid"),
    )
    .orderBy(F.col("cnt").desc_nulls_first())
)
