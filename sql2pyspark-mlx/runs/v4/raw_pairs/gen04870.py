from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy("salary", "id")
w2 = Window.partitionBy("level", "employee_level").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("level", "manager_id").orderBy(F.col("next_salary").desc_nulls_last(), "id")
w4 = Window.partitionBy("manager_id").orderBy("rank_in_group", "id")
w_ordered = Window.partitionBy("level").orderBy(F.col("salary").desc(), "id")
w6 = Window.orderBy("rank_in_group", "id").rowsBetween(-6, Window.currentRow)

filtered = (
    df_employees
    .filter(F.col("job_title").isNull())
    .select(
        "id",
        "full_name",
        "manager_id",
        "job_title",
        "salary",
        "is_remote",
        "level",
        F.lower("email").alias("email_lower"),
    )
)

enriched = (
    filtered
    .alias("e")
    .join(df_employees.alias("d"), F.col("e.manager_id") == F.col("d.id"), "inner")
    .select(
        "e.*",
        F.col("d.level").alias("employee_level"),
        F.col("d.full_name").alias("employee_full_name"),
    )
)

enriched_employees = (
    enriched
    .select(
        "*",
        F.lead("salary", 1, 0).over(w).alias("next_salary"),
        F.last("employee_level", ignorenulls=True).over(w2).alias("last_known_employee_level"),
    )
)

ranked = (
    enriched_employees
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    ranked
    .withColumn("next_salary_2", F.lead("salary", 1).over(w4))
    .withColumn("salary_bucket", F.ntile(10).over(w_ordered))
    .withColumn("salary_avg_6", F.avg("salary").over(w6))
)
