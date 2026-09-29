from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("full_name").like("%Rossi%"))
    .select(
        "id",
        "hire_date",
        "full_name",
        F.initcap(F.trim("level")).alias("level_title"),
    )
    .orderBy("hire_date")
)
