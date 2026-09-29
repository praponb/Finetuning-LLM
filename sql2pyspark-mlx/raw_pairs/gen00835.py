from pyspark.sql import functions as F

eligible = (
    df_departments
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.col("c.level") == "L5"), "left_anti")
)

result = (
    eligible
    .select(
        "id",
        "dept_name",
        "location",
        F.when(F.col("location").rlike("^(Bangkok|Tokyo)$"), "EXTENDED").when(F.col("location").rlike("^(Chiang Mai|New York)$"), "PRIORITY").otherwise("CORE").alias("location_zone"),
    )
)
