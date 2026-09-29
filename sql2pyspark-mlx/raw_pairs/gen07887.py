from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy(F.col("hire_date_period").desc_nulls_first(), "id")

filtered = (
    df_employees
    .filter((F.col("hire_date") > F.to_date(F.lit("2023-03-13"))) & F.col("level").isNotNull())
    .select("*", F.date_trunc("week", "hire_date").alias("hire_date_period"))
)

result = (
    filtered
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "hire_date",
        "is_remote",
    )
)
