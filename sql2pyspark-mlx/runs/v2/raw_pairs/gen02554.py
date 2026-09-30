from pyspark.sql import functions as F

base = df_employees.filter((F.year("hire_date") >= 2023) & F.col("full_name").isNotNull())

summary = (
    base
    .groupBy("is_remote", "level")
    .agg(F.max("hire_date").alias("latest_hire_date"))
)

result = (
    summary
    .select("is_remote", "latest_hire_date")
    .orderBy("latest_hire_date", "is_remote")
)
