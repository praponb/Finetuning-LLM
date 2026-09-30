from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy(F.year("hire_date"), "dept_id").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("job_title", "manager_id").orderBy("running_salary_2")
w4 = Window.orderBy(F.col("salary").desc())
w5 = Window.partitionBy("job_title").orderBy(F.col("running_salary").desc_nulls_last())
w6 = Window.partitionBy("manager_id", "job_title")
w_ordered = Window.orderBy(F.col("running_salary_share").desc_nulls_last())
w8 = Window.partitionBy("manager_id")

scored = (
    df_employees
    .filter(~F.col("full_name").like("Oli%"))
    .select(
        "id",
        "full_name",
        "email",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        F.sum("salary").over(w).alias("running_salary"),
        F.sum("salary").over(w2).alias("running_salary_2"),
    )
)

matched_keys = (
    scored
    .select("id", "manager_id")
    .intersectAll(
        df_employees
        .filter(F.col("job_title").isin(["Manager", "Engineer"]))
        .select("id", "manager_id")
    )
)

kept_rows = (
    scored
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.manager_id") == F.col("k.manager_id")), "inner")
    .select("s.*")
)

scored_employees = (
    kept_rows
    .select(
        "*",
        F.dense_rank().over(w3).alias("running_salary_2_dense_rank"),
        F.dense_rank().over(w4).alias("salary_dense_rank"),
        F.cume_dist().over(w5).alias("running_salary_cume_dist"),
        F.round((F.col("running_salary") / F.sum("running_salary").over(w6)), 4).alias("running_salary_share"),
    )
)

scored_employees_2 = (
    scored_employees
    .select(
        "id",
        "full_name",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        "running_salary_2",
        "running_salary_2_dense_rank",
        "salary_dense_rank",
        "running_salary_cume_dist",
        "running_salary_share",
        F.percent_rank().over(w_ordered).alias("running_salary_share_percent_rank"),
        F.round((F.col("salary_dense_rank") / F.sum("salary_dense_rank").over(w8)), 4).alias("salary_dense_rank_share"),
    )
)

result = (
    scored_employees_2
    .select(
        "job_title",
        "salary",
        "hire_date",
        "running_salary_2",
        "running_salary_2_dense_rank",
        "salary_dense_rank",
        "running_salary_cume_dist",
        "running_salary_share",
        "salary_dense_rank_share",
        F.when(F.col("job_title").rlike("^(Manager|Designer)$"), "EXTENDED").otherwise("CORE").alias("job_title_zone"),
    )
    .orderBy(F.col("salary").desc_nulls_first(), "job_title")
)
