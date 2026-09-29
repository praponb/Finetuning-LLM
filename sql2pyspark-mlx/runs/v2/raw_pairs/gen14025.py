from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("salary") > 77000) & (F.col("hire_date") <= "2023-09-01"))
    .withColumn("clean_email", F.trim("email"))
    .withColumn("hire_date_label", F.date_format("hire_date", "yyyy-MM"))
)
