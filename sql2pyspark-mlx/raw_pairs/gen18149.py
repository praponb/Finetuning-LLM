from pyspark.sql import functions as F

result = (
    df_employees
    .filter(~F.col("is_remote"))
    .select(
        "hire_date",
        "salary",
        F.date_format("hire_date", "yyyy-MM").alias("hire_date_label"),
        F.substring("full_name", 1, 3).alias("full_name_prefix"),
    )
    .orderBy(F.col("salary").desc(), "hire_date_label")
)
