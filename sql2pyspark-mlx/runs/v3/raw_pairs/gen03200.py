from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.lower(F.trim("full_name")) == "ivy chen")
    .groupBy(F.year("hire_date").alias("hire_year"))
    .agg(F.countDistinct("salary").alias("unique_managers"))
    .orderBy("hire_year")
)
