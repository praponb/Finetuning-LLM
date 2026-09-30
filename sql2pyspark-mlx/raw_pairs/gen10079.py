from pyspark.sql import functions as F

result = (
    df_employees
    .alias("src")
    .join(df_departments.alias("d"), F.col("src.dept_id") == F.col("d.id"), "inner")
    .groupingSets(
        [["location", "level"], ["location"]],
        F.col("d.location").alias("location"),
        F.col("src.level").alias("level"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.avg(F.col("src.salary")).alias("avg_salary"),
    )
)
