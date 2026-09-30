from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy("salary", "id")

filtered = (
    df_employees
    .filter(F.col("email").like("%@gmail.com"))
    .select(
        "id",
        "full_name",
        "email",
        "manager_id",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        F.lower("level").alias("level_lower"),
        F.add_months("hire_date", 6).alias("hire_date_plus_months"),
        F.coalesce("job_title", F.lit("UNKNOWN")).alias("job_title_clean"),
    )
)

result = (
    filtered
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)
