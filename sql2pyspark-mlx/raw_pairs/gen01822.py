from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("employee_job_title")
w2 = Window.orderBy("salary")
w3 = Window.partitionBy("dept_id", "level")
w4 = Window.orderBy(F.col("salary").desc())
w5 = Window.partitionBy("level").orderBy("salary_cume_dist")
w6 = Window.partitionBy("dept_id").orderBy(F.col("salary_cume_dist").desc_nulls_last())
w7 = Window.partitionBy("level").orderBy(F.col("salary_cume_dist_2").desc_nulls_last())

joined = (
    df_employees
    .alias("s")
    .join(df_employees.alias("b"), F.col("s.manager_id") == F.col("b.id"), "left")
    .select("s.*", F.col("b.job_title").alias("employee_job_title"))
)

enriched = (
    joined
    .alias("s")
    .join(df_employees.alias("d"), F.col("s.manager_id") == F.col("d.id"), "inner")
    .select(
        "s.*",
        F.col("d.level").alias("employee_level"),
        F.col("d.salary").alias("employee_salary"),
    )
)

kept_keys = (
    enriched
    .select("id")
    .intersectAll(
        df_employees
        .filter(F.year("hire_date") >= 2024)
        .select("id")
    )
)

verified = (
    enriched
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

cleaned = (
    verified
    .filter(F.col("employee_level").isin(["L3", "L2"]))
    .withColumn("clean_employee_level", F.trim("employee_level"))
)

scored = (
    cleaned
    .select(
        "full_name",
        "email",
        "dept_id",
        "job_title",
        "salary",
        "level",
        "employee_salary",
        "clean_employee_level",
        F.round((F.col("salary") / F.sum("salary").over(w)), 4).alias("salary_share"),
        F.cume_dist().over(w2).alias("salary_cume_dist"),
    )
)

scored_employees = (
    scored
    .filter(F.col("job_title") != "Analyst")
    .select(
        "*",
        F.round((F.col("employee_salary") / F.sum("employee_salary").over(w3)), 4).alias("employee_salary_share"),
        F.cume_dist().over(w4).alias("salary_cume_dist_2"),
        F.dense_rank().over(w5).alias("salary_cume_dist_dense_rank"),
        F.rank().over(w6).alias("salary_cume_dist_rank"),
    )
)

metrics = (
    scored_employees
    .select("*", F.percent_rank().over(w7).alias("salary_cume_dist_2_percent_rank"))
)

result = (
    metrics
    .select(
        "full_name",
        "email",
        "job_title",
        "salary",
        "level",
        "employee_salary",
        "clean_employee_level",
        "salary_share",
        "salary_cume_dist",
        F.when(F.col("full_name").rlike("^(Grace Kim|Emma Brown)$"), "CORE").when(F.col("full_name").rlike("^(Alice Wong|bob smith)$"), "PRIORITY").otherwise("OTHER_ZONE").alias("full_name_zone"),
    )
    .orderBy(F.col("full_name_zone").desc())
)
