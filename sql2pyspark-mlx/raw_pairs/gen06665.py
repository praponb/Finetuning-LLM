from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy("salary", "id")

ranked = (
    df_employees
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

result = (
    ranked
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        F.when(F.col("level").rlike("^(L2|L5)$"), "OTHER_ZONE").otherwise("GROUP_B").alias("level_zone"),
    )
    .orderBy(F.col("level").desc())
)
