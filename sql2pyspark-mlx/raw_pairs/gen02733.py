from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("employee_hire_date")
w2 = Window.partitionBy("job_title").orderBy(F.col("employee_hire_date").asc_nulls_first(), "id")

with_dims = (
    df_employees
    .alias("a")
    .join(df_employees.alias("e"), F.col("a.manager_id").eqNullSafe(F.col("e.id")), "left")
    .filter(~F.col("e.is_remote"))
    .select(
        "a.id",
        "a.full_name",
        "a.manager_id",
        "a.salary",
        "a.hire_date",
        "a.is_remote",
        "a.level",
        F.col("e.hire_date").alias("employee_hire_date"),
    )
)

enriched = (
    with_dims
    .alias("src")
    .join(df_employees.alias("p"), F.col("src.manager_id") == F.col("p.id"), "inner")
    .select(
        "src.id",
        "src.manager_id",
        "src.is_remote",
        "src.level",
        "src.employee_hire_date",
        F.col("p.full_name").alias("employee_full_name"),
        F.col("p.job_title"),
    )
)

ranked = (
    enriched
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "employee_hire_date", "job_title")
)

latest = (
    ranked
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "employee_hire_date")
)

result = latest.select("id", "employee_hire_date")
