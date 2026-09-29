from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy(F.col("salary").desc(), "id")
w2 = Window.partitionBy("level").orderBy("salary")
w3 = Window.partitionBy("dept_id").orderBy(F.col("hire_date").desc())

cleaned = (
    df_employees
    .filter(F.col("hire_date") > F.lit("2023-02-19").cast("date"))
    .withColumn("level_email_key", F.concat_ws("/", "level", "email"))
)

latest = (
    cleaned
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

joined = (
    latest
    .alias("s")
    .join(df_employees.alias("p"), F.col("s.manager_id") == F.col("p.id"), "left")
    .filter(F.col("p.is_remote"))
    .select(
        "s.*",
        F.col("p.job_title").alias("employee_job_title"),
        F.col("p.level").alias("employee_level"),
        F.col("p.hire_date").alias("employee_hire_date"),
    )
)

ranked = (
    joined
    .withColumn("row_num", F.rank().over(w2))
    .filter(F.col("row_num") == 1)
)

latest_employees = (
    ranked
    .withColumn("latest_rank", F.rank().over(w3))
    .filter(F.col("latest_rank") == 1)
)

wide = (
    latest_employees
    .groupBy("employee_job_title")
    .pivot("employee_level", ["L4", "L1"])
    .agg(F.avg("salary").alias("total"))
)

result = wide.select("employee_job_title", "L4", "L1")
