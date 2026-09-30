from pyspark.sql import functions as F

valid = (
    df_employees
    .filter((F.col("hire_date") >= F.lit("2023-07-23").cast("date")) & F.col("manager_id").isNotNull())
    .select(
        "id",
        "full_name",
        "dept_id",
        "hire_date",
        "is_remote",
        "level",
        F.when(F.col("level") == "L4", 1).otherwise(0).alias("is_l4"),
    )
)

result = (
    valid
    .alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "inner")
    .groupingSets(
        [["dept_name", "level"], ["dept_name"], []],
        F.col("d.dept_name").alias("dept_name"),
        F.col("e.level").alias("level"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.sum(F.col("e.is_l4")), 2).alias("total_is_l4"),
    )
)
